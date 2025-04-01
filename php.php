<?php

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

require_once __DIR__ . '/../app/app.php';

// load envs
$script_rate_limit_in_seconds = (int) env('SCRIPT_RATE_LIMIT_IN_SECONDS', 10);
$script_rate_limit_in_minutes = (int) env('SCRIPT_RATE_LIMIT_IN_MINUTES', $script_rate_limit_in_seconds * 60);
$host = env('DB_HOST');
$db = env('DB_DATABASE');

// instantiate db connection
$conn = new \PDO("mysql:host=$host;dbname=$db;charset=utf8", env('DB_USERNAME'), env('DB_PASSWORD'));
$conn->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

$callback = function (AMQPMessage $message) use ($script_rate_limit_in_seconds, $script_rate_limit_in_minutes, $conn) {
    // rate limit (seconds)
    $datetime = date('Ymd_His');
    $rateLimitFile = path_storage("rate-limit/{$datetime}.log");
    if (is_file($rateLimitFile)) {
        $content = file_get_contents($rateLimitFile);
        $content = explode(PHP_EOL, $content);
        $content = array_filter($content);

        if (count($content) >= $script_rate_limit_in_seconds) {
            $message->reject(requeue: true);
            echo ' [x] Rejected ' . PHP_EOL;
            return;
        }
    }
    // end rate limit

    // rate limit (minutes)
    $datetime = date('Ymd_Hi');
    $rateLimitFile = path_storage("rate-limit/{$datetime}.log");
    if (is_file($rateLimitFile)) {
        $content = file_get_contents($rateLimitFile);
        $content = explode(PHP_EOL, $content);
        $content = array_filter($content);

        if (count($content) >= $script_rate_limit_in_minutes) {
            $message->reject(requeue: true);
            echo ' [x] Rejected ' . PHP_EOL;
            return;
        }
    }
    // end rate limit

    $logFile = 'worker_send_message_' . date('Y-m-d') . '.log';
    file_put_contents(
        path_storage("logs/{$logFile}"),
        date('Y-m-d H:i:s') . ' :DEBUG: starting processing message' . PHP_EOL,
        FILE_APPEND
    );

    $data = $message->getBody();
    $data = json_decode($data, true);


    // try to send message
    try {
        $start_time = microtime(TRUE);

        $multiHandle = curl_multi_init();
        $curlHandles = [];
    
        foreach ($data as $index => $item) {
            $messenger_bot_broadcast_serial_id = $item['data']['messenger_bot_broadcast_serial'];
            $messenger_bot_broadcast_serial_send_id = $item['data']['messenger_bot_broadcast_serial_send'];
            $messenger_bot_subscriber_id = $item['data']['messenger_bot_subscriber'];
        
            // validate error limit in campaign
            $campaignData = $conn->query("SELECT posting_status, is_try_again FROM messenger_bot_broadcast_serial where id = {$messenger_bot_broadcast_serial_id};");
            $campaignData = $campaignData->fetch(\PDO::FETCH_OBJ);
       
            // if campaign is not sending, reject message
            if ($campaignData && $campaignData->posting_status == 4) {
                $message->ack();
                return;
            }
                
            $url = str_replace('\/', '/', $item['request']['url']);

            $ch = curl_init();
    
            curl_setopt_array($ch, [
                CURLOPT_URL => $url,
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_ENCODING  =>  'gzip,deflate',
                CURLOPT_RESOLVE => [
                    implode(':', ['genius-chat.com', '443', gethostbyname('genius-chat.com')]),
                ],
                CURLOPT_TIMEOUT => 30,
                CURLOPT_CUSTOMREQUEST => "POST",
                CURLOPT_POSTFIELDS => json_encode($item['request']['data']),
                CURLOPT_HTTPHEADER => [
                    "Content-Type: application/json",
                ],
            ]);
    
            curl_multi_add_handle($multiHandle, $ch);
            $curlHandles[$index] = $ch;
        }
    
        $running = null;
        do {
            curl_multi_exec($multiHandle, $running);
        } while ($running > 0);
    
        
        $responses = [];
        foreach ($curlHandles as $index => $ch) {
            $err = curl_error($ch);
    
            if ($err) {
                $responses[$index] = json_encode([
                    'error' => [
                        'message' => $err,
                        'code' => 0,
                    ]
                ]);
            } else {
                $responses[$index] = curl_multi_getcontent($ch);
            }
    
            curl_multi_remove_handle($multiHandle, $ch);
            curl_close($ch);
        }

        $end_time = microtime(TRUE);

        $logFile = 'worker_send_message_' . date('Y-m-d') . '.log';
        file_put_contents(
            path_storage("logs/{$logFile}"),
            date('Y-m-d H:i:s') . ' :DEBUG: send curl in ' . $end_time - $start_time . PHP_EOL,
            FILE_APPEND
        );

    } catch (\Throwable $th) {
        $responses[$index] = json_encode([
            'error' => [
                'message' => $th->getMessage(),
                'code' => 0,
            ]
        ]);

        $logFile = 'worker_send_message_' . date('Y-m-d') . '.log';
        file_put_contents(
            path_storage("logs/{$logFile}"),
            date('Y-m-d H:i:s') . ' :ERROR: ' . $th->getMessage() . ". On line: {$th->getLine()}" . PHP_EOL,
            FILE_APPEND
        );
    }





    foreach ($data as $index => $item) {
        $messenger_bot_broadcast_serial_id = $item['data']['messenger_bot_broadcast_serial'];
        $messenger_bot_broadcast_serial_send_id = $item['data']['messenger_bot_broadcast_serial_send'];
        $messenger_bot_subscriber_id = $item['data']['messenger_bot_subscriber'];

                // update message status based on response
        $msg = new AMQPMessage(json_encode([
            'messenger_bot_broadcast_serial' => $messenger_bot_broadcast_serial_id,
            'messenger_bot_broadcast_serial_send' => $messenger_bot_broadcast_serial_send_id,
            'messenger_bot_subscriber' => $messenger_bot_subscriber_id,
            'sent_time' => date('Y-m-d H:i:s'),
            'response' => json_decode($responses[$index], true),
        ]));
        $amqpConnection2 = new AMQPStreamConnection(env('AMQP_HOST'), env('AMQP_PORT'), env('AMQP_USERNAME'), env('AMQP_PASSWORD'), env('AMQP_VHOST'));
        $channel2 = $amqpConnection2->channel();
        $channel2->basic_publish($msg, '', 'broadcast-v2/update-status');

    }

    // add rate limit (seconds)
    $datetime = date('Ymd_His');
    $rateLimitFile = path_storage("rate-limit/{$datetime}.log");
    file_put_contents($rateLimitFile, time() . PHP_EOL, FILE_APPEND);

    // add rate limit (minutes)
    $datetime = date('Ymd_Hi');
    $rateLimitFile = path_storage("rate-limit/{$datetime}.log");
    file_put_contents($rateLimitFile, time() . PHP_EOL, FILE_APPEND);

    // acknowledge message
    $message->ack();
};

$amqpConnection = new AMQPStreamConnection(env('AMQP_HOST'), env('AMQP_PORT'), env('AMQP_USERNAME'), env('AMQP_PASSWORD'), env('AMQP_VHOST'));
$channel = $amqpConnection->channel();
$channel->basic_qos(null, 50, true);
$channel->basic_consume('broadcast-v2/send-messages', '', no_local: false, no_ack: false, exclusive: false, nowait: false, callback: $callback);

try {
    $channel->consume();
} catch (\Throwable $exception) {
    $logFile = 'worker_send_message_' . date('Y-m-d') . '.log';
    file_put_contents(
        path_storage("logs/{$logFile}"),
        date('Y-m-d H:i:s') . ' :ERROR: ' . $exception->getMessage() . ". On line: {$exception->getLine()}" . PHP_EOL,
        FILE_APPEND
    );
}

$channel->close();
$connection->close();
