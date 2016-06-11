<?php

require_once __DIR__ . '/config.php';
require_once __DIR__ . '/connection.php';
require_once __DIR__ . '/vendor/autoload.php';
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

$connection = new AMQPStreamConnection(MQ_HOST, MQ_PORT, MQ_LOGIN, MQ_PWD);
$channel = $connection->channel();

$channel->queue_declare(QUEUE_RESOLVE, false, true, false, false);
$channel->queue_declare(QUEUE_TRACK, false, true, false, false);

function trackContacts($contactIDs) {
    global $channel;
    $data = ["backgroundTask" => false, "contactIDs" => $contactIDs];
    $json = json_encode($data);
    $msg = new AMQPMessage($json, array('delivery_mode' => 2));
    $channel->basic_publish($msg, '', QUEUE_TRACK);
}

function resolveContacts($contactIDs) {
    global $channel;
    $data = ["backgroundTask" => false, "contactIDs" => $contactIDs];
    $json = json_encode($data);
    $msg = new AMQPMessage($json, array('delivery_mode' => 2));
    $channel->basic_publish($msg, '', QUEUE_RESOLVE);
    echo "Sent {$json} to " . QUEUE_RESOLVE . "\n";
}

function resolveContact($id) {
    try {
        $result = getConnection()
            ->prepare("UPDATE contacts SET status = 'RESOLVED' WHERE contact_id = ?")
            ->execute([$id]);
        return $result;
    }
    catch (Exception $e) {
        echo $e->getMessage()."\n";
        return false;
    }
}

function resolveContactsHandler($contactIDs, $backgroundTask = false, $batchLimit = 1) {
    $batches = array_chunk($contactIDs, $batchLimit);
    foreach ($batches as $k => $contactIDs) {
        if ($k == 0) {
            $track = [];
            foreach ($contactIDs as $id) {
                if (resolveContact($id) && !$backgroundTask) {
                    echo "Contact id:{$id} resolved\n";
                    $track[] = $id;
                }
            }
            if (!$backgroundTask && $track)
                trackContacts($track);
        }
        else {
            resolveContacts($contactIDs);
        }
    }
}

function trackContactsHandler($contactIDs) {
    foreach($contactIDs as $id) {
        echo "Contact id:{$id} tracked\n";
    }
};

$channel->basic_qos(null, 1, null);
$channel->basic_consume(QUEUE_RESOLVE, '', false, false, false, false, function($msg) {
        $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
        $msg = json_decode($msg->body);
        resolveContactsHandler($msg->contactIDs, $msg->backgroundTask, trackContactsLimit);
    });
$channel->basic_consume(QUEUE_TRACK, '', false, false, false, false, function($msg) {
        $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
        $msg = json_decode($msg->body);
        trackContactsHandler($msg->contactIDs);
    });

while(count($channel->callbacks)) {
    $channel->wait();
}

$channel->close();
$connection->close();

?>