<?php
	$hostname = "mqtt.example.com";
	$port = 8883;
	$ssl_cert = "/etc/ssl/certs/ca-certificates.crt";
	$username = "username";
	$password = "password";

	$base_topic = "zigbee2mqtt";

	$light_cluster[1]["lights"] = array($base_topic . "/Light_01" => "state_l2", $base_topic . "/Light_Laundry" => "state", $base_topic . "/Light_Toilet" => "state");
	$light_cluster[2]["lights"] = array($base_topic . "/Socket_04" => "state", $base_topic . "/Socket_05" => "state");

	$light_cluster[1]["topic"] = $base_topic . "/Light_Toilet";
	$light_cluster[2]["topic"] = $base_topic . "/Socket_04";

	$light_cluster[1]["lux_topic"] = $base_topic . "/Radar_01";
	$light_cluster[2]["lux_topic"] = $base_topic . "/Radar_04";

	$light_cluster[1]["lux"] = -1;
	$light_cluster[1]["raw"] = -1;

	$light_cluster[2]["lux"] = -1;
	$light_cluster[2]["raw"] = -1;

	$radar = array();

	$subs = array();
	$subs[] = array("topics" => array($base_topic . "/Light_Toilet", $base_topic . "/Radar_02", $base_topic . "/Radar_01"), "function_name" => "procMsg_Cluster1");
	$subs[] = array("topics" => array($base_topic . "/Socket_04", $base_topic . "/Radar_04", $base_topic . "/Radar_03"), "function_name" => "procMsg_Cluster2");

	$lastseen = array();
	$lastseen[1] = time();
	$lastseen[2] = time();
	$lastseen[3] = time();
	$lastseen[4] = time();
