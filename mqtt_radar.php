#!/usr/bin/php
<?php
	require_once("/var/www/mqtt-creds.php");
	require_once("/usr/src/MQTTv5Client/MQTThelper.php");

	$debug = true;

	subscribeAndWait($subs, $debug);

	function procMsg_Cluster1($topic, $message, $meta)
	{
		global $debug, $lastseen, $light_cluster, $radar;

		$cluster = 1;

                if($debug)
                {
                        echo "\nprocMsg_Cluster1() \$topic == $topic\n";
                        echo "\$message = $message\n";
                }

                if($message == null)
                {
                        echo "\$message == null\n";
                        return;
                }

		$id = intval(substr($topic, -2));

		$obj = json_decode($message, true);

                if($obj == null)
                {
                        echo "\$obj is null\n";
                        return;
                }

                if(!is_array($obj))
                {
                        echo "\$obj is not an array\n";
                        return;
                }

                if(count($obj) == 0)
                {
                        echo "count(\$obj) is zero\n";
                        return;
                }

		if($topic == "z2m2/Light_Toilet")
		{
			echo "\$light_cluster[$cluster]['on'] = ${obj['state']} == 'ON'\n";
			$light_cluster[$cluster]["on"] = $obj["state"] == "ON" ? 1 : 0;
			return;
		}

                if(!isset($obj["occupancy"]))
                {
                        echo "\$obj['occupancy'] is not set, skipping further checks...\n";
                        return;
                }

                if($debug)
                        echo "Setting \$radar[$id] to " . intval($obj["occupancy"]) . "\n";

                $radar[$id] = intval($obj["occupancy"]);

                if($topic == $light_cluster[$cluster]["lux_topic"])
                {
                        if($debug)
                                echo "Setting \$light_cluster[$cluster]['raw'] to " . intval($obj["illuminance_raw"]) . "\n";

                        $light_cluster[$cluster]["lux"] = intval($obj["illuminance"]);
                        $light_cluster[$cluster]["raw"] = intval($obj["illuminance_raw"]);
                }

                if($debug)
                {
                        echo "illuminance = " . $light_cluster[$cluster]["lux"] . "\n";
                        echo "illuminance_raw = " . $light_cluster[$cluster]["raw"] . "\n";
                }

                if($radar[$id] == 1)
                        $lastseen[$id] = time();

                if($id == 1 && $radar[$id] == 0 && time() - $lastseen[$id] >= 600 && $light_cluster[$cluster]["on"])
                {
                        if($debug)
                                echo "Lights may have been on too long, switch them off\n";

                        switchLights($cluster, "state", "OFF");
                        $lastseen[$id] = time();
                        return;
                }

                if(tooOld($obj))
                {
                        if($debug)
                                echo "Packet is too old, skipping...\n";
                        return;
                }

                if($id != 1)
                {
                        if($debug)
                                echo "\$id != 1\n";
                        return;
                }

                if(checkRadar($radar, $id, 1) && !$light_cluster[$cluster]["on"] && $light_cluster[$cluster]["raw"] < 1000)
                {
                        echo "turn cluster2 lights on\n";
                        switchLights($cluster);
                        return;
                }

                else if(checkRadar($radar, $id, 0) && $light_cluster[$cluster]["on"] && checkRadar($radar, 2, 0))
                {
                        echo "turn cluster1 lights off\n";
                        switchLights($cluster, "OFF");
                        return;
                }
	}

	function procMsg_Cluster2($topic, $message, $meta)
	{
		global $debug, $lastseen, $light_cluster, $radar;

		$cluster = 2;

                if($debug)
                {
                        echo "\nprocMsg_Cluster2() \$topic == $topic\n";
                        echo "\$message = $message\n";
                }

                if($message == null)
                {
                        echo "\$message == null\n";
                        return;
                }

		$id = intval(substr($topic, -2));

		$obj = json_decode($message, true);

                if($obj == null)
                {
                        echo "\$obj is null\n";
                        return;
                }

                if(!is_array($obj))
                {
                        echo "\$obj is not an array\n";
                        return;
                }

                if(count($obj) == 0)
                {
                        echo "count(\$obj) is zero\n";
                        return;
                }

		if($topic == "z2m2/Socket_04")
		{
			echo "\$light_cluster[$cluster]['on'] = ${obj['state']} == 'ON'\n";
			$light_cluster[$cluster]["on"] = $obj["state"] == "ON" ? 1 : 0;
			return;
		}

                if(!isset($obj["occupancy"]))
                {
                        echo "\$obj['occupancy'] is not set, skipping further checks...\n";
                        return;
                }

                if($debug)
                        echo "Setting \$radar[$id] to " . intval($obj["occupancy"]) . "\n";

                $radar[$id] = intval($obj["occupancy"]);

                if($topic == $light_cluster[$cluster]["lux_topic"])
                {
                        if($debug)
                                echo "Setting \$light_cluster[$cluster]['raw'] to " . intval($obj["illuminance_raw"]) . "\n";

                        $light_cluster[$cluster]["lux"] = intval($obj["illuminance"]);
                        $light_cluster[$cluster]["raw"] = intval($obj["illuminance_raw"]);
                }

                if($debug)
                {
                        echo "illuminance = " . $light_cluster[$cluster]["lux"] . "\n";
                        echo "illuminance_raw = " . $light_cluster[$cluster]["raw"] . "\n";
                }

                if($radar[$id] == 1)
                        $lastseen[$id] = time();

                if($id == 3 && $radar[$id] == 0 && time() - $lastseen[$id] >= 600 && $light_cluster[$cluster]["on"])
                {
                        if($debug)
                                echo "Lights may have been on too long, switch them off\n";

                        switchLights($cluster, "state", "OFF");
                        $lastseen[$id] = time();
                        return;
                }

                if(tooOld($obj))
                {
                        if($debug)
                                echo "Packet is too old, skipping...\n";
                        return;
                }

                if($id != 3)
                {
                        if($debug)
                                echo "\$id != 3\n";
                        return;
                }

                if(checkRadar($radar, $id, 1) && !$light_cluster[$cluster]["on"] && $light_cluster[$cluster]["raw"] < 1000)
                {
                        echo "turn cluster2 lights on\n";
                        switchLights($cluster);
                        return;
                }

                else if(checkRadar($radar, $id, 0) && $light_cluster[$cluster]["on"])
                {
                        echo "turn cluster2 lights off\n";
                        switchLights($cluster, "OFF");
                        return;
                }
	}
