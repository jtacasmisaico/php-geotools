<?php
namespace geography\db;

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

        $this->db->setAttribute( PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        $this->queries = $this->loadQueries($this->config['queriesFolder']);
        $this->builder = new FluentPDO($this->db);
    }

    public function query($name, $options = [], $format = PDO::FETCH_OBJ) {
        $stm = $this->getStatement($name, $options);
        $stm->execute();
        $result = [];
        while($tuple = $stm->fetch($format)) {
            $result[] = $tuple;
        }
        return $result;
    }

    public function all($name, $options = [], $format = PDO::FETCH_OBJ) {
        return $this->query($name, $options, $format);
    }

    public function one($name, $options = [], $format = PDO::FETCH_OBJ) {
        return $this->query($name, $options, $format)[0];
    }

    public function go($name, $options = [], $format = PDO::FETCH_OBJ) {
        return $this->query($name, $options, $format);
    }

    public function column($name, $options = []) {
        $stm = $this->getStatement($name, $options);
        $stm->execute();
        return $stm->fetchAll(PDO::FETCH_COLUMN, 0);
    }

    public function transaction($func) {
        $this->db->beginTransaction();
        try {
            $func();
            $this->db->commit();
        } catch(Exception $ex) {
            $this->db->rollBack();
            throw $ex;
        }
    }

    protected function getStatement($name, $options = []) {
        $sql = $this->queries[$name];
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
        foreach(glob($folder . '/*.yaml') as $path) {
            $queries = array_merge($queries, yaml_parse_file($path));
        }
        return $queries;
    }

}
