# beacon.py
# Creates a repeating thread to send a beacon message for announcements and testing
# (c) 2025 Jörg Hedtmann, DF3EI, <df3ei@db0kk.org>

import datetime
import time
import threading
from datetime import UTC
from modules.log import logger, CustomFormatter
from modules.system import wantAck, responseDelay, splitDelay

# Max size per packet (Meshtastic limit ~240 bytes)
MAX_CHUNK_SIZE = 240

class BeaconManager:
    def __init__(self):
        self.sequence_counter = 0
        self.beacon_message = ''
        self.nodeid = 0
        self.interface = None
        self.channel = None
        self.interval_minutes = -1.0
        self.active = False
        self.beacon_thread = None
        return

    def start_beacon(self, interval_minutes, channel, interface, nodeid, beacon_message):
        if self.active:
            logger.warning("Beacon is already running.")
            return

        logger.info(f'Scheduling beacon every {interval_minutes} minutes')
        self.active = True
        self.interval_minutes = float(interval_minutes) * 60.0
        self.channel = channel
        self.interface = interface
        self.nodeid = nodeid
        self.beacon_message = beacon_message

        # Start the background thread
        self.beacon_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.beacon_thread.start()
        logger.info("Beacon thread started.")
        return

    def stop_beacon(self):
        self.active = False
        logger.info("Beacon thread stopped.")
        return

    def _run_scheduler(self):
        next_call = time.time()
        while self.active:
            now = time.time()
            if now >= next_call:
                self.send_beacon()
                next_call = now + self.interval_minutes

            time.sleep(1)  # Prevent tight loop
        return

    def send_beacon(self):
        logger.debug(f'Attempting to send beacon: "{self.beacon_message}" to channel [{self.channel}] on interface [{self.interface.hostname}]')

        if not self.beacon_message or not isinstance(self.beacon_message, str) or len(self.beacon_message.strip()) == 0:
            logger.warning('Beacon text was empty or invalid. Not sending.')
            return

        timestamp = datetime.datetime.now(UTC)
        full_message = f'BEACON - {timestamp} [{self.sequence_counter}] {self.beacon_message}'
        logger.info(f'Formatted beacon: {full_message}')

        # --- Chunking Logic Added Here ---
        message_list = self.chunk_message(full_message)

        for idx, msg in enumerate(message_list):
            chunk_info = f"{idx + 1}/{len(message_list)}"
            prefix = f"Device:{self.interface.hostname} Channel:{self.channel} Chunker{chunk_info}"
            formatted_msg = msg.replace('\n', ' ')

            try:
                if self.nodeid == 0:
                    if wantAck:
                        logger.info(f"{prefix} " + CustomFormatter.red + "req.ACK SendingChannel:" + CustomFormatter.white + f" {formatted_msg}")
                        self.interface.sendText(text=msg, channelIndex=self.channel, wantAck=True)
                    else:
                        logger.info(f"{prefix} " + CustomFormatter.red + "SendingChannel:" + CustomFormatter.white + f" {formatted_msg}")
                        self.interface.sendText(text=msg, channelIndex=self.channel)
                else:
                    if wantAck:
                        logger.info(f"{prefix} " + CustomFormatter.red + "req.ACK Sending DM:" + CustomFormatter.white + f" {formatted_msg} To: {self.interface}")
                        self.interface.sendText(text=msg, destinationId=self.nodeid, wantAck=True)
                    else:
                        logger.info(f"{prefix} " + CustomFormatter.red + "Sending DM:" + CustomFormatter.white + f" {formatted_msg} To: {self.interface}")
                        self.interface.sendText(text=msg, destinationId=self.nodeid)

            except Exception as e:
                logger.error(f"Failed to send beacon chunk: {e}")

            if (idx + 1) % 4 == 0:
                time.sleep(responseDelay + 1)
                if (idx + 1) % 5 == 0:
                    logger.warning(f"System: throttling Interface{self.interface} at chunk {chunk_info}")

            time.sleep(splitDelay)

        self.sequence_counter += 1
        return

    def chunk_message(self, message, max_size=MAX_CHUNK_SIZE):
        """
        Splits a message into chunks of up to max_size, trying to keep words intact.
        """
        words = message.split(' ')
        chunks = []
        current_chunk = ""

        for word in words:
            if len(current_chunk) + len(word) + 1 <= max_size:
                if current_chunk:
                    current_chunk += ' ' + word
                else:
                    current_chunk = word
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = word

        if current_chunk:
            chunks.append(current_chunk)

        return chunks
