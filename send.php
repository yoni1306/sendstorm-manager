<?php

require_once __DIR__ . '/config.php';
require_once __DIR__ . '/connection.php';
require_once __DIR__ . '/vendor/autoload.php';
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

function queueTask($contactIds) {

    $ids = implode(",", array_map("intval", $contactIds));

    if ($result = getConnection()->query("SELECT * FROM contacts WHERE contact_id IN (" . $ids . ")")) {
        $connection = new AMQPStreamConnection(MQ_HOST, MQ_PORT, MQ_LOGIN, MQ_PWD);
        $channel = $connection->channel();

        $channel->queue_declare(QUEUE_RESOLVE, false, true, false, false);
        $channel->queue_declare(QUEUE_TRACK, false, true, false, false);

        $queues = [];
        while ($row = $result->fetch()) {
            switch ($row['status']) {
            case "RESOLVED":
                $queues[QUEUE_TRACK][] = $row['contact_id'];
                break;
            case "UNRESOLVED":
                $queues[QUEUE_RESOLVE][] = $row['contact_id'];
                break;
            }
        }

        foreach($queues as $queue => $ids) {
            $value = ["backgroundTask" => false, "contactIDs" => $ids];
            $value = json_encode($value);
            echo "Sending to {$queue}: {$value}\n";
            $msg = new AMQPMessage($value, array('delivery_mode' => 2));
            $channel->basic_publish($msg, '', $queue);
        }
    }
}

?>