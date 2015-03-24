<?php
namespace geography\db;

use Symfony\Component\Yaml\Parser;

use PDO;
use FluentPDO;

class Connection {
    private $db;
    private $queries;

    private $config = [
        'database' => 'postgis',
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
            $this->config['database'],
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

    public function __call($name, $options = []) {
        if(isset($options[0])) {
            $options = $options[0];
        }
        $query = $this->getQuery($name);
        $format = PDO::FETCH_OBJ;
        $multiple = true;
        $cls = null;
        if(is_array($query['returns']) && count($query['returns'])) {
            if(count($query['returns']) > 0) {
                $format = PDO::FETCH_CLASS;
                $cls = $query['returns'][0];
            }
        } else {
            $format = PDO::FETCH_CLASS;
            $multiple = false;
            $cls = $query['returns'];
        }
        $result = $this->query($query['sql'], $options, $format, $cls);

        if(!$multiple && count($result) == 0) {
            throw new NotFound(sprintf(
                'query: %s, options: %s',
                $query['sql'], print_r($options, true)
            ));
        }
        return $multiple ? $result : $result[0];
    }

    public function query($sql, $options = [], $format = PDO::FETCH_OBJ, $cls = null) {
        $stm = $this->getStatement($sql, $options);
        if($cls == null) $stm->setFetchMode($format);
        else $stm->setFetchMode($format, $cls);
        $stm->execute();
        return $stm->fetchAll();
    }

    public function column($name, $options = []) {
        $stm = $this->getStatement($this->getQuery($name)['sql'], $options);
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
        if(is_object($options)) {
            $options = get_object_vars($options);
        }

        // is this a non-associative array?
        if(count($options) && array_values($options) === $options) {
            $option_string = rtrim(str_repeat('?,', count($options)), ',');
            $sql = str_replace(':values', $option_string, $sql);
            $stm = $this->db->prepare($sql);
            foreach($options as $k => $v) {
                $stm->bindValue($k + 1, $v);
            }
            return $stm;
        }

        $stm = $this->db->prepare($sql);
        error_log(print_r($options, true), 4);
        foreach($options as $k => $v) {
            if($v instanceof \DateTime) {
                $v = $v->format(\DateTime::ISO8601);
            }

            if(is_bool($v)) {
                $v = (int) $v;
            }
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

    public function getQuery($name) {
        if(!array_key_exists($name, $this->queries)) {
            throw new DatabaseException(sprintf('Query "%s" not found', $name));
        }
        $q = $this->queries[$name];
        if(!is_string($q)) {
            return $q;
        }
        return [
            'sql' => $q,
            'returns' => []
        ];
    }

}
