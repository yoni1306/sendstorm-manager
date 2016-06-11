<?php

function getConnection() {
    static $db = false;
    static $timeout = 1;

    try {
        if (!$db) {
            $dsn = "mysql:host=" . DB_HOST . ";dbname=" . DB_NAME . ";charset=utf8";
            $db = new PDO($dsn, DB_USER, DB_PWD, [
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
            ]);
        }

        $db->query("SELECT 1");
        $timeout = 0;
    }
    catch (Exception $e) {
        usleep($timeout);
        $timeout += 100000;
        return getConnection();
    }

    return $db;
}

?>