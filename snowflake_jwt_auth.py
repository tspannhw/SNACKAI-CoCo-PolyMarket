#!/usr/bin/env python3
"""
Snowflake Authentication Module

Supports two authentication methods:
1. JWT Key-Pair Authentication (requires RSA key generation)
2. Programmatic Access Token (PAT) (generate in Snowflake UI)

References:
- JWT: https://docs.snowflake.com/en/developer-guide/sql-api/guide#using-key-pair-authentication
- PAT: https://docs.snowflake.com/en/user-guide/authentication-programmatic-tokens
"""

import jwt
import time
import logging
import requests
from datetime import datetime, timedelta, timezone
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from hashlib import sha256
import base64
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class SnowflakeJWTAuth:
    """Handles authentication for Snowflake (JWT or PAT)."""

    def __init__(self, config: Dict):
        """
        Initialize authentication.

        Args:
            config: Dictionary with account, user, and one of:
                    - pat: Programmatic Access Token
                    - private_key_file/private_key_path: Path to RSA private key
        """
        self.config = config
        self.account = config['account'].upper()
        self.user = config['user'].upper()

        if 'pat' in config and config['pat']:
            self.auth_method = 'pat'
            self.pat = config['pat']
            logger.info(f"PAT authentication initialized for user: {self.user}")
        elif config.get('private_key_file') or config.get('private_key_path'):
            self.auth_method = 'jwt'
            self.private_key_file = config.get('private_key_file') or config.get('private_key_path')
            self.private_key = self._load_private_key()
            self.qualified_username = f"{self.account}.{self.user}"
            logger.info(f"JWT auth initialized for user: {self.qualified_username}")
        else:
            raise ValueError(
                "No authentication method configured. "
                "Provide either 'pat' or 'private_key_file' in config."
            )

    def _load_private_key(self):
        """Load private key from PEM/PKCS8 file."""
        try:
            with open(self.private_key_file, 'rb') as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=None,
                    backend=default_backend()
                )
            logger.info(f"Private key loaded from {self.private_key_file}")
            return private_key
        except FileNotFoundError:
            logger.error(f"Private key file not found: {self.private_key_file}")
            raise
        except Exception as e:
            logger.error(f"Error loading private key: {e}")
            raise

    def generate_jwt_token(self) -> str:
        """Generate a JWT token for Snowflake authentication."""
        public_key_bytes = self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        public_key_fp = 'SHA256:' + base64.b64encode(sha256(public_key_bytes).digest()).decode()

        now = datetime.now(timezone.utc)
        payload = {
            'iss': f"{self.qualified_username}.{public_key_fp}",
            'sub': self.qualified_username,
            'iat': int(now.timestamp()),
            'exp': int((now + timedelta(hours=1)).timestamp())
        }

        token = jwt.encode(payload, self.private_key, algorithm='RS256')
        logger.debug("JWT token generated")
        return token

    def get_scoped_token(self) -> str:
        """Get authentication token (PAT or JWT-exchanged OAuth token)."""
        if self.auth_method == 'pat':
            return self.pat
        elif self.auth_method == 'jwt':
            return self._get_jwt_oauth_token()
        else:
            raise ValueError(f"Unknown auth method: {self.auth_method}")

    def _get_jwt_oauth_token(self) -> str:
        """Exchange JWT for a scoped OAuth token."""
        logger.info("Exchanging JWT for OAuth token...")
        jwt_token = self.generate_jwt_token()

        account = self.config['account'].lower()
        token_url = f"https://{account}.snowflakecomputing.com/oauth/token"
        role = self.config.get('role', 'PUBLIC').upper()

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': jwt_token,
            'scope': f'session:role:{role}'
        }

        try:
            response = requests.post(token_url, headers=headers, data=data, timeout=30)
            response.raise_for_status()

            # Snowflake may return a JSON object with access_token or a raw JWT string
            try:
                token_data = response.json()
                access_token = token_data.get('access_token')
                if access_token:
                    logger.info("OAuth token obtained (JSON response)")
                    return access_token
            except (ValueError, KeyError):
                pass

            # Raw JWT token in response body
            raw_token = response.text.strip()
            if raw_token:
                logger.info("OAuth token obtained (raw token response)")
                return raw_token

            raise ValueError("Empty response from token endpoint")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get OAuth token: {e}")
            raise

    def get_authorization_header(self) -> str:
        """Get Authorization header value."""
        token = self.get_scoped_token()
        return f"Bearer {token}"

    def get_bearer_token(self) -> str:
        """Get bearer token for API calls."""
        return self.get_scoped_token()

    def get_scoped_token_payload(self, scope: str = "") -> Dict:
        """Get payload for scoped token request."""
        if self.auth_method == 'pat':
            return {}
        jwt_token = self.generate_jwt_token()
        return {
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': jwt_token,
            'scope': scope
        }


if __name__ == '__main__':
    import json
    logging.basicConfig(level=logging.INFO)
    try:
        with open('snowflake_config.json', 'r') as f:
            config = json.load(f)
        auth = SnowflakeJWTAuth(config)
        token = auth.get_scoped_token()
        print(f"Token obtained (length: {len(token)})")
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
