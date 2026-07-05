import json
import secrets
from typing import Tuple

class MT5EABuilder:
    @staticmethod
    def generate_pairing_credentials(user_id: str, account_number: str) -> Tuple[str, str]:
        """
        Generates a secure pairing token and unique gateway channel ID.
        """
        pairing_token = secrets.token_urlsafe(32)
        # Unique queue channel mapping back to this specific terminal session
        gateway_channel = f"mt5:bridge:{user_id}:{account_number}"
        return pairing_token, gateway_channel

    @staticmethod
    def create_ea_config_file(gateway_url: str, pairing_token: str, gateway_channel: str) -> str:
        """
        Returns a JSON configuration payload that can be read by a generic pre-compiled EA,
        or compiled into an installer setup.
        """
        config = {
            "GATEWAY_URL": gateway_url,
            "PAIRING_TOKEN": pairing_token,
            "CHANNEL": gateway_channel,
            "HEARTBEAT_INTERVAL_MS": 3000
        }
        return json.dumps(config, indent=2)