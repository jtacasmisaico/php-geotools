<?php
namespace geography\db;

use Symfony\Component\Yaml\Parser;

use PDO;
use FluentPDO;

class Connection {
    private $db;
    private $queries;

    private $config = [
        'schema' => 'postgis',
        'server' => 'localhost',
        'port' => 5432,
        'username' => 'postgis',
        'password' => 'postgis',
        'queriesFolder' => 'db/queries'
    ];

    public $builder;

    public function __construct($config = []) {
        // Override the default config values
        $this->config = array_merge($this->config, $config);
        // Build the connection string from the $config field.
        $connectionString = sprintf('pgsql:host=%s;dbname=%s;port=%d',
            $this->config['server'],
            $this->config['schema'],
            $this->config['port']
        );

        $this->db = new PDO($connectionString,
            $this->config['username'],
            $this->config['password']
        );

        $this->db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->db->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);

        $this->queries = $this->loadQueries($this->config['queriesFolder']);
        $this->builder = new FluentPDO($this->db);
    }

    public function query($sql, $options = [], $format = PDO::FETCH_OBJ) {
        $stm = $this->getStatement($sql, $options);
        try {
            $stm->execute();
        } catch(\PDOException $ex) {
            throw new DatabaseException(
                "Invalid query $sql, or invalid query options",
                is_int($ex->getCode()) ? $ex->getCode() : 1337, // stupid PDO!
                $ex
            );
        }
        $result = [];
        while($tuple = $stm->fetch($format)) {
            $result[] = $tuple;
        }
        return $result;
    }

    public function all($name, $options = [], $format = PDO::FETCH_OBJ) {
        return $this->query($this->getQuery($name), $options, $format);
    }

    public function one($name, $options = [], $format = PDO::FETCH_OBJ) {
        $data = $this->query($this->getQuery($name), $options, $format);
        if(!isset($data) || count($data) === 0) {
            throw new NotFound(
                "query: $name, options:" . print_r($options, true)
            );
        }
        return $data[0];
    }

    public function go($name, $options = [], $format = PDO::FETCH_OBJ) {
        return $this->query($this->getQuery($name), $options, $format);
    }

    public function column($name, $options = []) {
        $stm = $this->getStatement($this->getQuery($name), $options);
        $stm->execute();
        return $stm->fetchAll(PDO::FETCH_COLUMN, 0);
    }

    public function transaction($func) {
        $this->db->beginTransaction();
        try {
            $func();
            $this->db->commit();
        } catch(\Exception $ex) {
            $this->db->rollBack();
            throw $ex;
        }
    }

    protected function getStatement($sql, $options = []) {
        // is this a non-associative array?
        if(array_values($options) === $options) {
            $option_string = rtrim(str_repeat('?,', count($options)), ',');
            $sql = str_replace(':values', $option_string, $sql);
            $stm = $this->db->prepare($sql);
            foreach($options as $k => $v) {
                $stm->bindValue($k + 1, $v);
            }
            return $stm;
        }

        $stm = $this->db->prepare($sql);
        foreach($options as $k => $v) {
            $stm->bindValue(':' . $k, $v);
        }
        return $stm;
    }

    protected function loadQueries($folder) {
        $queries = [];
        $yaml = new Parser();
        foreach(glob($folder . '/*.yaml') as $path) {
            $queries = array_merge($queries, 
                $yaml->parse(file_get_contents($path))
            );
        }
        return $queries;
    }

    protected function getQuery($name) {
        if(!array_key_exists($name, $this->queries)) {
            throw new DatabaseException(sprintf('Query "%s" not found', $name));
        }
        return $this->queries[$name];
    }

}
