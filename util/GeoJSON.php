<?php
namespace geography\util;
use geoPHP;

class GeoJSON {
    public static function features($records, $geometry_attr = ['geometry' => 'json']) {
        $result = [
            'type' => 'FeatureCollection',
            'features' => []
        ];

        foreach($records as $record) {
            $result['features'][] = static::feature($record, $geometry_attr);
        }

        return $result;
    }

    public static function feature($record, $geometry_attr = ['geometry' => 'json']) {
        if(!is_array($record)) {
            $arr = get_object_vars($record);
        } else {
            $arr = $record;
        }

        list($attr, $format) = static::geometryAttribute($geometry_attr);
        $geometry = geoPHP::load($arr[$attr], $format)->out('json', true);

        unset($arr[$attr]);

        return [
            'type' => 'Feature',
            'geometry' => $geometry,
            'properties' => $arr,
        ];
    }

    public static function object($feature, $geometry_attr = ['geometry' => 'array']) {
        list($attr, $format) = static::geometryAttribute($geometry_attr);
        $obj = $feature;
        if(is_string($feature)) {
            $obj = json_decode($feature);
        }

        $props = $obj->properties;
        $props->geometry = $obj->geometry;
        if($format === 'json') {
            $props->$attr = json_encode($props->$attr);
        }

        if($format === 'geoPHP') {
            $props->$attr = geoPHP::load($props->$attr, 'json');
        }

        return $props;
    }

    public static function objects($collection, $geometry_attr = ['geometry' => 'array']) {
        $objects = [];
        foreach(json_decode($collection)->features as $feature) {
            $objects[] = static::object($feature, $geometry_attr);
        }
        return $objects;
    }

    public static function arr($feature, $geometry_attr = ['geometry' => 'array']) {
        return get_object_vars(static::object($feature, $geometry_attr));
    }

    private static function geometryAttribute($input = []) {
        // default name and format
        $name = 'geometry';
        $format = 'json';
        if(!isset($input)) { $input = []; }
        if(!is_array($input)) { $input = [$input]; }

        foreach($input as $key => $value) {
            if(is_string($key)) {
                // The key is a string, so it isn't a number, which means
                // it's of the form "geometry_name" => "format"
                $name = $key;
                $format = $value;
            } else {
                // Otherwise, it's just a normal 0-indexed array value
                $format = $key;
            }
        }

        return [$name, $format];
    }
}
