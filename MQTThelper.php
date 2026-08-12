<?php
	require_once("/usr/src/MQTTv5Client/MQTTv5Client.php");

	function mqttconnect($client_id, $debug=false)
	{
		global $hostname, $port, $ssl_cert, $username, $password;

		if(false)
		{
			echo "\$hostname == $hostname\n";
			echo "\$port == $port\n";
			echo "\$username == $username\n";
			echo "\$password == $password\n";
			echo "\$ssl_cert == $ssl_cert\n";
			//exit;
		}

		$use_ssl = false;
		if(isset($ssl_cert) && file_exists($ssl_cert))
			$use_ssl = true;

		$mqtt = new MQTTv5Client($hostname, $port, $client_id, $use_ssl);
		$mqtt->setDebug($debug);

		try
		{
			$mqtt->connect($username, $password, false);
			if($debug)
				echo "Connected!\n";

			return $mqtt;
		} catch (MQTTConnectException $e) {
			echo $e->getMessage() . "\n";
			if($e->getReasonCode() === 0x86)
			{
				echo "Invalid username or password\n";
				return false;
			} elseif ($e->getReasonCode() === 0x8A) {
				echo "Your account has been banned, talk to your sysadmin...\n";
				exit;
			}

			echo $e->getReasonLabel();
			echo $e->getReasonString();

			return false;
		} catch (RuntimeException $e) {
			echo "Network error: " . $e->getMessage() . "\n";
			return false;
		}
	}

	function mqttpublish($topic, $payload, $retain=true, $debug=false)
	{
		$mqtt = mqttconnect(uniqid(), $debug);
		if(!$mqtt)
		{
			logIt("Failed to connect", true);
			return false;
		}

		if(is_array($payload))
			$payload = json_encode($payload);

		logIt("Topic: $topic");
		logIt("Payload: $payload");

		$mqtt->publish($topic, $payload, 0, $retain);
		$mqtt->disconnect();

		return true;
	}

	function subscribeAll($mqtt, $subs, $debug)
	{
		foreach($subs as $sub)
		{
			foreach($sub["topics"] as $topic)
			{
				logIt("Subscribing to $topic and binding to " . $sub["function_name"]);
				$mqtt->subscribe($topic, 0, Closure::fromCallable($sub["function_name"]));
			}
		}
	}

	function subscribeAndWait(&$mqtt, $subs, $debug=false)
	{
		while(true)
		{
			try
			{
				if($mqtt)
				{
					subscribeAll($mqtt, $subs, $debug);
					$mqtt->loop(true);
				} else {
					echo "\$mqtt is false will reconnect after 5 seconds...\n";
					sleep(5);
					$mqtt = mqttconnect(uniqid(), $debug);
				}
			} catch (MQTTConnectionLostException $e) {
				echo "Connection lost: {$e->getMessage()} — reconnecting...\n";
				$mqtt = mqttconnect(uniqid(), $debug);
			} catch (MQTTConnectException $e) {
				echo "Broker rejected connection: {$e->getReasonLabel()} — retrying in 10s\n";
				sleep(10);
				$mqtt = mqttconnect(uniqid(), $debug);
			} catch (Exception $e) {
				echo "Connection lost: {$e->getMessage()} — reconnecting...\n";
				$mqtt = mqttconnect(uniqid(), $debug);
			}
		}
	}

	function mqttget($client_id, $topic, $debug=false)
	{
		$mqtt = mqttconnect($client_id, $debug);
		if(!$mqtt)
		{
			logIt("Failed to connect, skipping...", true);
			return false;
		}

		$ret = $mqtt->readRetained($topic);

		$obj = null;
		if(count($ret))
		{
			foreach($ret as $msg)
			{
				if(isset($msg["message"]) && strlen($msg["message"]) > 5)
				{
					$obj = json_decode($msg["message"], true);
					break;
				}
			}
		}

		$mqtt->disconnect();

		if($obj != null && is_array($obj) && count($obj) > 0)
			return $obj;

		return false;
	}

	function getValuesFrom($client_id, $topic, $debug=false)
	{
		$ret = mqttget($client_id, $topic, $debug);

		if($debug)
			var_dump($ret);

		if($ret === null || trim($ret) == "")
			return false;

		$obj = json_decode($ret, true);
		if($obj === null || !is_array($obj) || count($obj) == 0)
			return false;

		return $obj;
	}

	function tooOld($obj)
	{
		global $debug;

		if(!isset($obj["last_seen"]))
		{
			echo "\$obj doesn't have a last_seen field.\n";
			return true;
		}

		if(trim($obj["last_seen"]) == "")
		{
			echo "\$obj['last_seen'] is an empty field.\n";
			return true;
		}

		$date = new DateTimeImmutable($obj["last_seen"]);
		$now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
		$diffInSeconds = $now->getTimestamp() - $date->getTimestamp();
		if($diffInSeconds > 60)
		{
			if($debug)
				echo "Not a new packet, skipping...\n";

			return true;
		}

		return false;
	}

	function switchLights($cluster="1", $newstate="ON")
	{
		global $debug, $light_cluster;

		echo "Set cluster$cluster lights $newstate\n";

		foreach($light_cluster[$cluster]["lights"] as $light => $state)
		{
			$topic = $light . "/set";
			$msg = array();
			$msg[$state] = $newstate;

			$payload = json_encode($msg);

			if($debug)
				echo "Sending $payload to $topic\n";

			mqttpublish($topic, $payload);
		}
	}

	function checkRadar(&$radar, $id, $val, $occupancy)
	{
		if($radar === null)
		{
			logIt("checkRadar() \$radar is null", true);
			return true;
		}

		if(!isset($radar[$id]))
		{
			$radar[$id] = $occupancy;
			logIt("checkRadar() !isset(\$radar[$id]) ... setting \$radar[$id] = $occupancy");
			return true;
		}

		if($radar[$id] == $val)
		{
			logIt("checkRadar() \$radar[$id] == $val");
			return true;
		}

		logIt("checkRadar() \$radar[$id] != $val");
		return false;
	}

	function delayedStart()
	{
		global $argv;

		if(in_array("--from-cron", $argv))
		{
			$r = new \Random\Randomizer();
			sleep($r->getInt(30, 300));
		}
	}

	function isTrue($arr, $index)
	{
		if($arr === null || !is_array($arr) || !count($arr) || !isset($arr[$index]))
			return false;

		if($arr[$index])
			return true;

		return false;
	}

	function logIt($str, $logAnyway=false)
	{
		global $debug;

		if($debug || $logAnyway)
			echo date("Y-m-d H:i:s") . " - " . $str . "\n";
	}
