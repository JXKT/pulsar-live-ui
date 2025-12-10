import argparse
import sys
from typing import NoReturn

# Now importing AppSettings directly from the package
from pulsar_listen import PulsarManager, PulsarConfig, AppSettings

def get_config(args: argparse.Namespace) -> PulsarConfig:
    settings = AppSettings()

    return {
        "broker_service_url": args.broker or settings.broker_service_url,
        "admin_service_url": args.admin or settings.admin_service_url,
        "token": args.token or settings.token,
    }

# ... (The rest of your cli.py commands remain exactly the same)