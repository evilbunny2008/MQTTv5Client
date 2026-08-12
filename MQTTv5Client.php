<?php
/**
 * MQTTv5Client
 * -------------
 * A minimal, dependency-free MQTT 5.0 client for PHP built on raw TCP sockets.
 *
 * Supports:
 *  - connect() / disconnect()  (with MQTT 5 properties + reason codes)
 *  - publish() with QoS 0/1, retain flag, and common properties
 *  - subscribe() with a per-topic-filter callback and MQTT 5 subscription options
 *  - loop() to process incoming PUBLISH/PINGRESP/SUBACK/etc.
 *  - readRetained() convenience helper to fetch retained message(s) for a topic
 *
 * Notes:
 *  - QoS 2 is not implemented.
 *  - Property support covers the commonly used ones (user properties, message/session
 *    expiry, content type, payload format indicator, reason string). Enough to talk to
 *    any compliant MQTT 5 broker (Mosquitto 2+, EMQX, HiveMQ, AWS IoT Core, etc.)
 *    without pulling in a full spec implementation.
 *
 * Example:
 *   $mqtt = new MQTTv5Client('broker.example.com', 1883, 'my-client-id');
 *   $mqtt->connect('user', 'pass');
 *   $mqtt->publish('home/livingroom/lamp', 'ON', 0, true); // retained
 *   $mqtt->subscribe('home/#', 0, function ($topic, $message, $meta) {
 *	   echo "[$topic] $message" . ($meta['retained'] ? ' (retained)' : '') . PHP_EOL;
 *   });
 *   $mqtt->loop(true); // blocks forever, dispatching messages to callbacks
 */

/**
 * Thrown when the broker rejects a CONNECT with a non-zero reason code (MQTT 5 CONNACK).
 * Lets calling code branch on the specific reason instead of parsing an exception string.
 */
class MQTTConnectException extends RuntimeException
{
	protected int $reasonCode;
	protected string $reasonString;

	/** @var array<int,string> Common MQTT 5 CONNACK reason codes -> human-readable label */
	protected const REASON_LABELS = [
		0x00 => 'Success',
		0x80 => 'Unspecified error',
		0x81 => 'Malformed packet',
		0x82 => 'Protocol error',
		0x83 => 'Implementation specific error',
		0x84 => 'Unsupported protocol version',
		0x85 => 'Client identifier not valid',
		0x86 => 'Bad username or password',
		0x87 => 'Not authorized',
		0x88 => 'Server unavailable',
		0x89 => 'Server busy',
		0x8A => 'Banned',
		0x8C => 'Bad authentication method',
		0x90 => 'Topic name invalid',
		0x95 => 'Packet too large',
		0x97 => 'Quota exceeded',
		0x99 => 'Payload format invalid',
		0x9A => 'Retain not supported',
		0x9B => 'QoS not supported',
		0x9C => 'Use another server',
		0x9D => 'Server moved',
		0x9F => 'Connection rate exceeded',
	];

	public function __construct(int $reasonCode, string $reasonString = '')
	{
		$this->reasonCode = $reasonCode;
		$this->reasonString = $reasonString;

		$label = self::REASON_LABELS[$reasonCode] ?? 'Unknown reason';
		$message = sprintf('MQTT 5 CONNECT refused: %s (reason code 0x%02X)', $label, $reasonCode);
		if($reasonString !== '') {
			$message .= ' — ' . $reasonString;
		}

		parent::__construct($message, $reasonCode);
	}

	/** The raw numeric reason code from the CONNACK packet (e.g. 0x86). */
	public function getReasonCode(): int
	{
		return $this->reasonCode;
	}

	/** The optional human-readable reason string the broker sent, if any ('' if none). */
	public function getReasonString(): string
	{
		return $this->reasonString;
	}

	/** The short built-in label for this reason code (e.g. "Bad username or password"). */
	public function getReasonLabel(): string
	{
		return self::REASON_LABELS[$this->reasonCode] ?? 'Unknown reason';
	}
}

/**
 * Thrown when a write/read reveals the underlying socket has already been closed by the
 * peer — typically because the broker timed out the keep-alive (loop() wasn't called
 * often enough) or the network dropped. Catch this to trigger a reconnect.
 */
class MQTTConnectionLostException extends RuntimeException
{
}

class MQTTv5Client
{
	// ---- MQTT 5 property identifiers (subset in use here) ----
	protected const PROP_PAYLOAD_FORMAT_INDICATOR		= 0x01;
	protected const PROP_MESSAGE_EXPIRY_INTERVAL		= 0x02;
	protected const PROP_CONTENT_TYPE			= 0x03;
	protected const PROP_SUBSCRIPTION_IDENTIFIER		= 0x0B;
	protected const PROP_SESSION_EXPIRY_INTERVAL		= 0x11;
	protected const PROP_ASSIGNED_CLIENT_ID			= 0x12;
	protected const PROP_SERVER_KEEP_ALIVE			= 0x13;
	protected const PROP_REASON_STRING			= 0x1F;
	protected const PROP_RECEIVE_MAXIMUM			= 0x21;
	protected const PROP_TOPIC_ALIAS_MAXIMUM		= 0x22;
	protected const PROP_TOPIC_ALIAS			= 0x23;
	protected const PROP_MAXIMUM_QOS			= 0x24;
	protected const PROP_RETAIN_AVAILABLE			= 0x25;
	protected const PROP_USER_PROPERTY			= 0x26;
	protected const PROP_MAXIMUM_PACKET_SIZE		= 0x27;
	protected const PROP_WILDCARD_SUB_AVAILABLE		= 0x28;
	protected const PROP_SUBSCRIPTION_ID_AVAILABLE		= 0x29;
	protected const PROP_SHARED_SUB_AVAILABLE		= 0x2A;

	/** @var string */
	protected $host;
	/** @var int */
	protected $port;
	/** @var string */
	protected $clientId;
	/** @var resource|null */
	protected $socket;
	/** @var int */
	protected $keepAlive = 60;
	/** @var float */
	protected $lastActivity = 0;
	/** @var float Timestamp of the last packet WE sent — this is what matters for keep-alive, not received traffic */
	protected $lastPacketSent = 0;
	/** @var int */
	protected $msgId = 1;
	/** @var callable|null Fallback: function(string $topic, string $message, array $meta) */
	protected $onMessage;
	/** @var array<string,callable> Per-topic-filter callbacks, keyed by raw subscribed filter string */
	protected $topicCallbacks = [];
	/** @var bool */
	protected $debug = false;
	/** @var int socket connect timeout seconds */
	protected $connectTimeout = 10;
	/** @var bool TLS connection */
	protected $useTls = false;
	/** @var array Properties returned by the broker in CONNACK (assigned client id, server keep-alive, etc.) */
	public $serverProperties = [];

	public function __construct(string $host, int $port = 1883, string $clientId = '', bool $useTls = false)
	{
		$this->host = $host;
		$this->port = $port;
		$this->clientId = $clientId !== '' ? $clientId : $this->generateClientId();
		$this->useTls = $useTls;
	}

	public function setDebug(bool $debug): void
	{
		$this->debug = $debug;
	}

	protected function generateClientId(): string
	{
		return 'php-mqtt5-' . substr(bin2hex(random_bytes(6)), 0, 12);
	}

	public function log(string $msg, bool $sendAnyway=false): void
	{
		if($this->debug || $sendAnyway)
			fwrite(STDERR, date("h:i:sa").' [MQTTv5Client] ' . $msg . PHP_EOL);
	}

	// ---------------------------------------------------------------------
	// Connection handling
	// ---------------------------------------------------------------------

	/**
	 * Open the TCP/TLS socket and perform the MQTT 5 CONNECT handshake.
	 *
	 * @param int|null $sessionExpiryInterval Seconds the broker should keep the session after disconnect (null = omit).
	 */
	public function connect(
		?string $username = null,
		?string $password = null,
		bool $cleanStart = true,
		int $keepAlive = 60,
		?int $sessionExpiryInterval = null): bool
	{
		$this->keepAlive = $keepAlive;

		$this->log("Keep alive set to {$keepAlive}");

		$scheme = $this->useTls ? 'tls' : 'tcp';
		$address = sprintf('%s://%s:%d', $scheme, $this->host, $this->port);

		$this->socket = @stream_socket_client(
			$address,
			$errno,
			$errstr,
			$this->connectTimeout,
			STREAM_CLIENT_CONNECT
		);

		if(!$this->socket)
			throw new RuntimeException("Unable to connect to $address: $errstr ($errno)");

		stream_set_timeout($this->socket, $this->connectTimeout);

		// ---- Build CONNECT packet ----
		$protocolName = $this->encodeString('MQTT');
		$protocolLevel = chr(5); // MQTT 5.0

		$connectFlags = 0x00;
		if($cleanStart)
			$connectFlags |= 0x02;

		if($username !== null)
			$connectFlags |= 0x80;

		if($password !== null)
			$connectFlags |= 0x40;

		// CONNECT properties
		$properties = '';
		if($sessionExpiryInterval !== null)
			$properties .= chr(self::PROP_SESSION_EXPIRY_INTERVAL) . $this->encodeUint32($sessionExpiryInterval);

		$propertiesEncoded = $this->encodeVariableByteInteger(strlen($properties)) . $properties;

		$payload = $this->encodeString($this->clientId);
		if($username !== null)
			$payload .= $this->encodeString($username);

		if($password !== null)
			$payload .= $this->encodeString($password);

		$variableHeader = $protocolName . $protocolLevel . chr($connectFlags) . $this->encodeUint16($keepAlive) . $propertiesEncoded;
		$body = $variableHeader . $payload;

		$packet = chr(0x10) . $this->encodeVariableByteInteger(strlen($body)) . $body;
		$this->write($packet);

		// ---- Read CONNACK ----
		$header = $this->readBytes(1);
		$type = ord($header) >> 4;
		$remainingLength = $this->readRemainingLength();
		$ackBody = $remainingLength > 0 ? $this->readBytes($remainingLength) : '';

		if($type !== 2)
			throw new RuntimeException('Expected CONNACK, got packet type ' . $type);

		// Byte 0: connect ack flags (bit 0 = session present)
		$reasonCode = ord($ackBody[1] ?? "\x00");
		$rest = substr($ackBody, 2);
		[$props, ] = $this->decodeProperties($rest);
		$this->serverProperties = $props;

		if($reasonCode !== 0)
			throw new MQTTConnectException($reasonCode, $props['reasonString'] ?? '');

		// MQTT 5: broker may override the keep-alive interval we requested.
		if(isset($props['serverKeepAlive']))
		{
			$this->keepAlive = $props['serverKeepAlive'];
			$this->log("Broker overrode keep-alive to {$this->keepAlive}s", true);
		}

		$this->lastActivity = microtime(true);
		$this->log("Connected to $address as {$this->clientId} (MQTT 5)");

		return true;
	}

	public function disconnect(int $reasonCode = 0x00): void
	{
		if($this->socket)
		{
			// Normal disconnect (reason code 0) can be sent with zero remaining length.
			if($reasonCode === 0x00)
			{
				$this->write(chr(0xE0) . chr(0x00));
			} else {
				$body = chr($reasonCode) . chr(0x00); // no properties
				$this->write(chr(0xE0) . $this->encodeVariableByteInteger(strlen($body)) . $body);
			}

			fclose($this->socket);

			$this->socket = null;
			$this->log('Disconnected');
		}
	}

	public function isConnected(): bool
	{
		return is_resource($this->socket) && !feof($this->socket);
	}

	/** Seconds since any packet was last sent or received (useful for verifying keep-alive health). */
	public function getSecondsSinceLastActivity(): float
	{
		return microtime(true) - $this->lastActivity;
	}

	/** Seconds since WE last sent a packet to the broker — this is what actually matters for keep-alive. */
	public function getSecondsSinceLastPacketSent(): float
	{
		return microtime(true) - $this->lastPacketSent;
	}

	// ---------------------------------------------------------------------
	// Publish
	// ---------------------------------------------------------------------

	/**
	 * Publish a message to a topic.
	 *
	 * @param int   $qos		0 or 1 supported.
	 * @param array $properties Optional: ['contentType' => string, 'messageExpiryInterval' => int,
	 *						   'payloadFormatIndicator' => 0|1, 'userProperties' => [['key','value'], ...]]
	 */
	public function publish(string $topic, string $message, int $qos = 0, bool $retain = false, array $properties = []): void
	{
		$flags = 0x30; // PUBLISH
		$flags |= ($qos & 0x03) << 1;
		if($retain)
			$flags |= 0x01;

		$variableHeader = $this->encodeString($topic);
		if($qos > 0)
		{
			$id = $this->nextMsgId();
			$variableHeader .= $this->encodeUint16($id);
		}

		$variableHeader .= $this->encodePublishProperties($properties);

		$body = $variableHeader . $message;
		$packet = chr($flags) . $this->encodeVariableByteInteger(strlen($body)) . $body;
		$this->write($packet);

		if($qos > 0)
			$this->waitForPacketType(4, 5); // PUBACK

		$this->log("Published to $topic (qos=$qos, retain=" . ($retain ? '1' : '0') . '): ' . $message);
	}

	protected function encodePublishProperties(array $properties): string
	{
		$encoded = '';
		if(isset($properties['payloadFormatIndicator']))
			$encoded .= chr(self::PROP_PAYLOAD_FORMAT_INDICATOR) . chr($properties['payloadFormatIndicator'] ? 1 : 0);

		if(isset($properties['messageExpiryInterval']))
			$encoded .= chr(self::PROP_MESSAGE_EXPIRY_INTERVAL) . $this->encodeUint32($properties['messageExpiryInterval']);

		if(isset($properties['contentType']))
			$encoded .= chr(self::PROP_CONTENT_TYPE) . $this->encodeString($properties['contentType']);

		if(!empty($properties['userProperties']))
			foreach($properties['userProperties'] as [$key, $value])
				$encoded .= chr(self::PROP_USER_PROPERTY) . $this->encodeString($key) . $this->encodeString($value);

		return $this->encodeVariableByteInteger(strlen($encoded)) . $encoded;
	}

	// ---------------------------------------------------------------------
	// Subscribe
	// ---------------------------------------------------------------------

	/**
	 * Subscribe to a topic filter and register a callback for messages that arrive on it.
	 * Callback signature: function(string $topic, string $message, array $meta): void
	 *   $meta = ['qos' => int, 'retained' => bool, 'properties' => array]
	 *
	 * @param array $options Optional subscription options:
	 *   ['noLocal' => bool, 'retainAsPublished' => bool, 'retainHandling' => 0|1|2]
	 */
	public function subscribe(string $topicFilter, int $qos = 0, ?callable $callback = null, array $options = []): void
	{
		$id = $this->nextMsgId();

		$subOptions = $qos & 0x03;
		if(!empty($options['noLocal']))
			$subOptions |= 0x04;

		if(!empty($options['retainAsPublished']))
			$subOptions |= 0x08;

		if(isset($options['retainHandling']))
			$subOptions |= ($options['retainHandling'] & 0x03) << 4;

		$propertiesEncoded = $this->encodeVariableByteInteger(0); // no SUBSCRIBE properties by default
		$body = $this->encodeUint16($id) . $propertiesEncoded . $this->encodeString($topicFilter) . chr($subOptions);
		$packet = chr(0x82) . $this->encodeVariableByteInteger(strlen($body)) . $body;
		$this->write($packet);

		if($callback !== null)
			$this->topicCallbacks[$topicFilter] = $callback;

		$this->waitForPacketType(9, 5); // SUBACK

		$this->log("Subscribed to $topicFilter (qos=$qos)");
	}

	/** Set a fallback callback used when no topic-specific callback matches. */
	public function setDefaultMessageCallback(callable $callback): void
	{
		$this->onMessage = $callback;
	}

	// ---------------------------------------------------------------------
	// Loop / message processing
	// ---------------------------------------------------------------------

	/**
	 * Process incoming data.
	 *
	 * @param bool  $forever If true, blocks and loops until disconnect()/error.
	 * @param float $timeout Seconds to wait for a single read when $forever is false (0 = non-blocking poll).
	 */
	public function loop(bool $forever = false, float $timeout = 1.0): void
	{
		do
		{
			$this->pingIfNeeded();

			$read = [$this->socket];
			$write = $except = null;
			$sec = (int) floor($timeout);
			$usec = (int) (($timeout - $sec) * 1_000_000);

			$changed = @stream_select($read, $write, $except, $sec, $usec);
			if($changed === false)
				throw new RuntimeException('stream_select() failed');

			if($changed > 0)
				$this->handleIncomingPacket();
		} while($forever && $this->isConnected());
	}

	public function pingIfNeeded(): void
	{
		if($this->keepAlive == 0 || microtime(true) - $this->lastPacketSent < $this->keepAlive * 0.8)
			return;

		$this->write(chr(0xC0) . chr(0x00)); // PINGREQ
		$this->lastActivity = microtime(true);
		$this->log('Sent PINGREQ');
	}

	protected function handleIncomingPacket(): void
	{
		$header = $this->readBytes(1, true);
		if($header === '')
			return;

		$byte1 = ord($header);
		$type = $byte1 >> 4;
		$flags = $byte1 & 0x0F;
		$remainingLength = $this->readRemainingLength();
		$body = $remainingLength > 0 ? $this->readBytes($remainingLength) : '';

		$this->lastActivity = microtime(true);

		switch($type)
		{
			case 3: // PUBLISH
				$this->dispatchPublish($body, $flags);
				break;
			case 13: // PINGRESP
				$this->log('Received PINGRESP');
				break;
			case 9: // SUBACK
				$this->log('Received SUBACK');
				break;
			case 4: // PUBACK
				$this->log('Received PUBACK');
				break;
			case 14: // DISCONNECT (server-initiated, MQTT 5 only)
				$reasonCode = strlen($body) > 0 ? ord($body[0]) : 0;
				$this->log("Server sent DISCONNECT, reason code $reasonCode");
				if($this->socket)
				{
					fclose($this->socket);
					$this->socket = null;
				}
				break;
			default:
				$this->log("Received packet type $type (ignored)");
		}
	}

	protected function dispatchPublish(string $body, int $flags): void
	{
		$qos = ($flags >> 1) & 0x03;
		$retained = (bool) ($flags & 0x01);

		$topicLen = (ord($body[0]) << 8) | ord($body[1]);
		$topic = substr($body, 2, $topicLen);
		$offset = 2 + $topicLen;

		$msgId = null;
		if($qos > 0)
		{
			$msgId = (ord($body[$offset]) << 8) | ord($body[$offset + 1]);
			$offset += 2;
		}

		// PUBLISH properties
		[$properties, $consumed] = $this->decodeProperties(substr($body, $offset));
		$offset += $consumed;

		$message = substr($body, $offset);

		if($qos === 1 && $msgId !== null)
		{
			$ackBody = $this->encodeUint16($msgId) . chr(0x00) . chr(0x00); // reason 0, no properties
			$this->write(chr(0x40) . $this->encodeVariableByteInteger(strlen($ackBody)) . $ackBody);
		}

		$meta = [
			'qos' => $qos,
			'retained' => $retained,
			'properties' => $properties,
		];

		$callback = $this->matchTopicCallback($topic) ?? $this->onMessage;
		if($callback !== null)
			$callback($topic, $message, $meta);
		else
			$this->log("No callback for $topic, message dropped: $message");
	}

	protected function matchTopicCallback(string $topic): ?callable
	{
		foreach($this->topicCallbacks as $filter => $callback)
			if($this->topicMatches($filter, $topic))
				return $callback;

		return null;
	}

	protected function topicMatches(string $filter, string $topic): bool
	{
		$filterParts = explode('/', $filter);
		$topicParts = explode('/', $topic);

		foreach($filterParts as $i => $part)
		{
			if($part === '#')
				return true;

			if(!isset($topicParts[$i]))
				return false;

			if($part !== '+' && $part !== $topicParts[$i])
				return false;
		}

		return count($filterParts) === count($topicParts);
	}

	// ---------------------------------------------------------------------
	// Convenience: read retained message(s) for a topic
	// ---------------------------------------------------------------------

	/**
	 * Subscribe briefly to a topic filter and collect any retained message(s) the
	 * broker immediately sends back, then return them.
	 *
	 * @return array<int,array{topic:string,message:string,properties:array}>
	 */
	public function readRetained(string $topicFilter, float $waitSeconds = 2.0, int $qos = 0): array
	{
		$collected = [];
		// retainHandling = 0 => send retained messages at subscribe time (the default anyway,
		// but set explicitly since MQTT 5 lets brokers suppress them otherwise).
		$this->subscribe($topicFilter, $qos, function (string $topic, string $message, array $meta) use (&$collected)
		{
			if($meta['retained'])
				$collected[] = ['topic' => $topic, 'message' => $message, 'properties' => $meta['properties']];
		}, ['retainHandling' => 0]);

		$deadline = microtime(true) + $waitSeconds;
		while(microtime(true) < $deadline)
			$this->loop(false, 0.2);

		unset($this->topicCallbacks[$topicFilter]);
		return $collected;
	}

	// ---------------------------------------------------------------------
	// MQTT 5 properties decoding
	// ---------------------------------------------------------------------

	/**
	 * Decode an MQTT 5 properties block starting at the beginning of $data.
	 *
	 * @return array{0: array, 1: int} [decoded properties, total bytes consumed including the length prefix]
	 */
	protected function decodeProperties(string $data): array
	{
		[$length, $lengthBytes] = $this->decodeVariableByteInteger($data);
		$propsData = substr($data, $lengthBytes, $length);
		$totalConsumed = $lengthBytes + $length;

		$result = ['userProperties' => []];
		$pos = 0;
		$end = strlen($propsData);

		while($pos < $end)
		{
			$id = ord($propsData[$pos]);
			$pos++;

			switch($id)
			{
				case self::PROP_PAYLOAD_FORMAT_INDICATOR:
					$result['payloadFormatIndicator'] = ord($propsData[$pos]);
					$pos += 1;
					break;

				case self::PROP_MESSAGE_EXPIRY_INTERVAL:
				case self::PROP_SESSION_EXPIRY_INTERVAL:
				case self::PROP_MAXIMUM_PACKET_SIZE:
					$value = $this->decodeUint32(substr($propsData, $pos, 4));
					$result[$this->propName($id)] = $value;
					$pos += 4;
					break;

				case self::PROP_SERVER_KEEP_ALIVE:
				case self::PROP_RECEIVE_MAXIMUM:
				case self::PROP_TOPIC_ALIAS_MAXIMUM:
				case self::PROP_TOPIC_ALIAS:
					$value = (ord($propsData[$pos]) << 8) | ord($propsData[$pos + 1]);
					$result[$this->propName($id)] = $value;
					$pos += 2;
					break;

				case self::PROP_MAXIMUM_QOS:
				case self::PROP_RETAIN_AVAILABLE:
				case self::PROP_WILDCARD_SUB_AVAILABLE:
				case self::PROP_SUBSCRIPTION_ID_AVAILABLE:
				case self::PROP_SHARED_SUB_AVAILABLE:
					$result[$this->propName($id)] = ord($propsData[$pos]);
					$pos += 1;
					break;

				case self::PROP_CONTENT_TYPE:
				case self::PROP_ASSIGNED_CLIENT_ID:
				case self::PROP_REASON_STRING:
					$strLen = (ord($propsData[$pos]) << 8) | ord($propsData[$pos + 1]);
					$value = substr($propsData, $pos + 2, $strLen);
					$result[$this->propName($id)] = $value;
					$pos += 2 + $strLen;
					break;

				case self::PROP_USER_PROPERTY:
					$keyLen = (ord($propsData[$pos]) << 8) | ord($propsData[$pos + 1]);
					$key = substr($propsData, $pos + 2, $keyLen);
					$pos += 2 + $keyLen;
					$valLen = (ord($propsData[$pos]) << 8) | ord($propsData[$pos + 1]);
					$value = substr($propsData, $pos + 2, $valLen);
					$pos += 2 + $valLen;
					$result['userProperties'][] = [$key, $value];
					break;

				case self::PROP_SUBSCRIPTION_IDENTIFIER:
					[$subId, $subIdBytes] = $this->decodeVariableByteInteger(substr($propsData, $pos));
					$result['subscriptionIdentifier'] = $subId;
					$pos += $subIdBytes;
					break;

				default:
					// Unknown property: we can't safely skip a variable-length one without
					// knowing its type, so stop parsing further properties defensively.
					$pos = $end;
					break;
			}
		}

		return [$result, $totalConsumed];
	}

	protected function propName(int $id): string
	{
		static $map = [
			self::PROP_MESSAGE_EXPIRY_INTERVAL	=> 'messageExpiryInterval',
			self::PROP_SESSION_EXPIRY_INTERVAL	=> 'sessionExpiryInterval',
			self::PROP_MAXIMUM_PACKET_SIZE		=> 'maximumPacketSize',
			self::PROP_SERVER_KEEP_ALIVE		=> 'serverKeepAlive',
			self::PROP_RECEIVE_MAXIMUM		=> 'receiveMaximum',
			self::PROP_TOPIC_ALIAS_MAXIMUM		=> 'topicAliasMaximum',
			self::PROP_TOPIC_ALIAS			=> 'topicAlias',
			self::PROP_MAXIMUM_QOS			=> 'maximumQos',
			self::PROP_RETAIN_AVAILABLE		=> 'retainAvailable',
			self::PROP_WILDCARD_SUB_AVAILABLE	=> 'wildcardSubscriptionAvailable',
			self::PROP_SUBSCRIPTION_ID_AVAILABLE	=> 'subscriptionIdentifiersAvailable',
			self::PROP_SHARED_SUB_AVAILABLE		=> 'sharedSubscriptionAvailable',
			self::PROP_CONTENT_TYPE			=> 'contentType',
			self::PROP_ASSIGNED_CLIENT_ID		=> 'assignedClientIdentifier',
			self::PROP_REASON_STRING		=> 'reasonString',
		];

		return $map[$id] ?? ('prop_' . $id);
	}

	// ---------------------------------------------------------------------
	// Low-level socket + MQTT encoding helpers
	// ---------------------------------------------------------------------

	protected function write(string $data): void
	{
		if(!$this->isConnected())
			throw new RuntimeException('Not connected');

		if(feof($this->socket))
		{
			$this->socket = null;
			throw new MQTTConnectionLostException('Socket already closed by peer before write (broker likely timed out the keep-alive)');
		}

		$written = @fwrite($this->socket, $data);
		if($written === false || ($written === 0 && strlen($data) > 0))
		{
			$this->socket = null;
			throw new MQTTConnectionLostException('Write failed — connection lost (broken pipe / peer closed)');
		}

		$this->lastPacketSent = microtime(true);
	}

	protected function readBytes(int $length, bool $nonBlockingFirstByte = false): string
	{
		if($length === 0)
			return '';

		$data = '';
		$remaining = $length;
		while($remaining > 0)
		{
			$chunk = fread($this->socket, $remaining);
			if($chunk === false || ($chunk === '' && feof($this->socket)))
			{
				if($nonBlockingFirstByte && $data === '')
					return '';

				throw new RuntimeException('Connection closed while reading');
			}

			$data .= $chunk;
			$remaining -= strlen($chunk);
		}

		return $data;
	}

	protected function readRemainingLength(): int
	{
		$multiplier = 1;
		$value = 0;
		do
		{
			$byte = ord($this->readBytes(1));
			$value += ($byte & 0x7F) * $multiplier;
			$multiplier *= 128;
		} while(($byte & 0x80) !== 0);

		return $value;
	}

	protected function encodeVariableByteInteger(int $length): string
	{
		$bytes = '';
		do
		{
			$byte = $length % 128;
			$length = intdiv($length, 128);
			if($length > 0)
				$byte |= 0x80;

			$bytes .= chr($byte);
		} while($length > 0);

		return $bytes;
	}

	/** @return array{0:int,1:int} [decoded value, bytes consumed] */
	protected function decodeVariableByteInteger(string $data): array
	{
		$multiplier = 1;
		$value = 0;
		$pos = 0;
		do
		{
			$byte = ord($data[$pos]);
			$value += ($byte & 0x7F) * $multiplier;
			$multiplier *= 128;
			$pos++;
		} while(($byte & 0x80) !== 0);

		return [$value, $pos];
	}

	protected function encodeString(string $str): string
	{
		return $this->encodeUint16(strlen($str)) . $str;
	}

	protected function encodeUint16(int $value): string
	{
		return chr(($value >> 8) & 0xFF) . chr($value & 0xFF);
	}

	protected function encodeUint32(int $value): string
	{
		return pack('N', $value);
	}

	protected function decodeUint32(string $bytes): int
	{
		$unpacked = unpack('N', $bytes);
		return $unpacked[1];
	}

	protected function nextMsgId(): int
	{
		$id = $this->msgId;
		$this->msgId = ($this->msgId % 65535) + 1;
		return $id;
	}

	/** Blocks (up to $timeoutSec) until a packet of the given type is read, processing others along the way. */
	protected function waitForPacketType(int $expectedType, float $timeoutSec): void
	{
		$deadline = microtime(true) + $timeoutSec;
		while(microtime(true) < $deadline)
		{
			$read = [$this->socket];
			$write = $except = null;
			if(@stream_select($read, $write, $except, 0, 200000) > 0)
			{
				$header = $this->readBytes(1);
				$type = ord($header) >> 4;
				$flags = ord($header) & 0x0F;
				$remainingLength = $this->readRemainingLength();
				$body = $remainingLength > 0 ? $this->readBytes($remainingLength) : '';
				$this->lastActivity = microtime(true);

				if($type === $expectedType)
					return;

				if($type === 3)
					$this->dispatchPublish($body, $flags);
			}
		}

		$this->log("Timed out waiting for packet type $expectedType");
	}

	public function __destruct()
	{
		if($this->isConnected())
			$this->disconnect();
	}
}
