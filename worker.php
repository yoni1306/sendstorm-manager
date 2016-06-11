<?php

require_once __DIR__ . '/config.php';
require_once __DIR__ . '/vendor/autoload.php';
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

$connection = new AMQPStreamConnection(MQ_HOST, MQ_PORT, MQ_LOGIN, MQ_PWD);
$channel = $connection->channel();

$channel->queue_declare(QUEUE_RESOLVE, false, true, false, false);
$channel->queue_declare(QUEUE_TRACK, false, true, false, false);

function getConnection() {
    static $db = false;
    if (!$db || (!$db->ping()))
        $db = new mysqli(DB_HOST, DB_USER, DB_PWD, DB_NAME);
    return $db;
}

function trackContacts($contactIDs) {
    global $channel;
	$data = ["backgroundTask" => true, "contactIDs" => $contactIDs];
	$json = json_encode($data);
	$msg = new AMQPMessage($json, array('delivery_mode' => 2));
    $channel->basic_publish($msg, '', QUEUE_TRACK);
}

function resolveContacts($contactIDs) {
    global $channel;
	$data = ["backgroundTask" => true, "contactIDs" => $contactIDs];
	$json = json_encode($data);
	$msg = new AMQPMessage($json, array('delivery_mode' => 2));
    $channel->basic_publish($msg, '', QUEUE_RESOLVE);
	echo "Sent {$json} to " . QUEUE_RESOLVE . "\n";
}

function resolveContact($id) {
    $stmt = getConnection()->prepare("UPDATE contacts SET status = 'RESOLVED' WHERE contact_id = ?");
    if (!$stmt)
        return false;
    $stmt->bind_param("i", $id);
    $result = $stmt->execute();
    return $result;
}

function resolveContactsHandler($contacts, $backgroundTask = true, $batchLimit = 1) {
	$batches = array_chunk($contacts, $batchLimit);
	foreach ($batches as $k => $contacts) {
		if ($k == 0) {
			$track = [];
			foreach ($contacts as $id) {
				if (resolveContact($id) && $backgroundTask) {
					echo "Contact id:{$id} resolved\n";
					$track[] = $id;
				}
			}
			if ($backgroundTask && $track)
				trackContacts($track);
		}
		else {
			resolveContacts($contacts);
		}
	}
}

function trackContactsHandler($contacts) {
	foreach($contacts as $id) {
        echo "Contact id:{$id} tracked\n";
	}
};

$channel->basic_qos(null, 1, null);
$channel->basic_consume(QUEUE_RESOLVE, '', false, false, false, false, function($msg) {
		$msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
		$msg = json_decode($msg->body);
		resolveContactsHandler($msg->contactIDs, $msg->backgroundTask, 2);
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