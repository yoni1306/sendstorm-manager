<?php

//require_once __DIR__ . '/config.php';
//require_once __DIR__ . '/connection.php';
require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/utility_functions.php';
require_once __DIR__ . '/vendor/whatsapp/chat-api/src/whatsprot.class.php';
require_once __DIR__ . '/vendor/whatsapp/chat-api/src/exception.php';


use PhpAmqpLib\Connection\AMQPStreamConnection;
//use PhpAmqpLib\Message\AMQPMessage;

$connection = new AMQPStreamConnection(MQ_HOST, MQ_PORT, MQ_LOGIN, MQ_PWD);
$channel = $connection->channel();

$channel->queue_declare(QUEUE_RESOLVE, false, true, false, false);
$channel->queue_declare(QUEUE_TRACK, false, true, false, false);

//function trackContacts($contactIDs)
//{
//    global $channel;
//    $data = ["backgroundTask" => false, "contactIDs" => $contactIDs];
//    $json = json_encode($data);
//    $msg = new AMQPMessage($json, array('delivery_mode' => 2));
//    $channel->basic_publish($msg, '', QUEUE_TRACK);
//}
//
//function resolveContacts($contactIDs)
//{
//    global $channel;
//    $data = ["backgroundTask" => false, "contactIDs" => $contactIDs];
//    $json = json_encode($data);
//    $msg = new AMQPMessage($json, array('delivery_mode' => 2));
//    $channel->basic_publish($msg, '', QUEUE_RESOLVE);
//    echo "Sent {$json} to " . QUEUE_RESOLVE . "\n";
//}
//
//function resolveContact($id)
//{
//    try {
//        $result = getConnection()
//            ->prepare("UPDATE contacts SET status = 'RESOLVED' WHERE contact_id = ?")
//            ->execute([$id]);
//        return $result;
//    } catch (Exception $e) {
//        echo $e->getMessage() . "\n";
//        return false;
//    }
//}

function resolveContactsHandler($channel_id, $contact_ids)
{
    $ch = getChannel($channel_id);
    $contact_phone_numbers = getContactPhoneNumbers($contact_ids);

    print_r($ch['secret']);
    print_r($contact_phone_numbers);

    if ($ch && $contact_phone_numbers) {
        $username = "nickname";
        $password = $ch['secret'];
        $u = $contact_phone_numbers;
        $numbers = [];

        if (!is_array($u)) {
            $u = [$u];
        }

        foreach ($u as $number) {
            if ($number[0] != '+') {
                //add leading +
                $number = "+$number";
            }
            $numbers[] = $number;
        }
        //event handler
        /**
         * @param $result SyncResult
         */
        function onSyncResult($result)
        {
            foreach ($result->existing as $number) {
                echo "$number exists " , "\n";

                $phone_number = substr($number, 0, strpos($number, "@"));

                markContactAsValid($phone_number);
            }
            foreach ($result->nonExisting as $number) {
                echo "$number does not exist ", "\n";

                $phone_number = substr($number, 0, strpos($number, "@"));

                markContactAsInvalid($phone_number);
            }
            die(); //to break out of the while(true) loop
        }

        try {
            $wa = new WhatsProt($username, 'WhatsApp', false);
            //bind event handler
            $wa->eventManager()->bind('onGetSyncResult', 'onSyncResult');
            $wa->connect();
            $wa->loginWithPassword($password);
            //send dataset to server
            $wa->sendSync($numbers);
            //wait for response
            while (true) {
                $wa->pollMessage();
            }
        } catch (LoginFailureException $e){
            echo 'Caught LoginFailureException, marking channel as blocked', "\n";
            markChannelAsBlocked($channel_id);
        } catch(Exception $e){
            echo 'Caught exception: ',  $e->getMessage(), "\n";
        }
    }
}


function trackContactsHandler($contactIDs)
{
    foreach ($contactIDs as $id) {
        echo "Contact id:{$id} tracked\n";
    }
}

;

$channel->basic_qos(null, 1, null);
$channel->basic_consume(QUEUE_RESOLVE, '', false, false, false, false, function ($msg) {
    $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
    $msg = json_decode($msg->body);
    resolveContactsHandler($msg->channelID, $msg->contactIDs);
});
//$channel->basic_consume(QUEUE_TRACK, '', false, false, false, false, function ($msg) {
//    $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
//    $msg = json_decode($msg->body);
//    trackContactsHandler($msg->channelID, $msg->contactIDs);
//});

while (count($channel->callbacks)) {
    $channel->wait();
}

$channel->close();
$connection->close();

?>