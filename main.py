import argparse
import sys
from typing import NoReturn

# Importing directly from your library package
from pulsar_listen import PulsarManager, PulsarConfig, AppSettings

def get_config(args: argparse.Namespace) -> PulsarConfig:
    """
    Combines Pydantic Settings (Env/Defaults) with CLI Arguments.
    CLI Arguments take precedence.
    """
    settings = AppSettings()

    return {
        "broker_service_url": args.broker or settings.broker_service_url,
        "admin_service_url": args.admin or settings.admin_service_url,
        "token": args.token or settings.token,
    }

def cmd_list_tenants(manager: PulsarManager) -> None:
    print("Fetching tenants...")
    if tenants := manager.get_tenants():
        print(f"Found {len(tenants)} tenants:")
        for t in tenants:
            print(f" - {t}")
    else:
        print("No tenants found (or connection failed). check your Admin URL.")

def cmd_list_topics(manager: PulsarManager, args: argparse.Namespace) -> None:
    namespace = args.namespace
    print(f"Fetching topics for {namespace}...")

    if topics := manager.get_topics(namespace):
        print(f"Found {len(topics)} topics:")
        for t in topics:
            print(f" - {t}")
    else:
        print(f"No topics found in {namespace}.")

def cmd_listen(manager: PulsarManager, args: argparse.Namespace) -> None:
    topic = args.topic
    limit = args.limit
    print(f"Listening to {topic} (Limit: {limit} messages)...")
    print("-" * 60)

    try:
        with manager.create_listener(topic, limit=limit) as listener:
            for msg in listener:
                print(f"[{msg.publish_time.strftime('%H:%M:%S')}] ID: {msg.message_id}")
                if msg.key:
                    print(f"Key: {msg.key}")
                print(f"Props: {msg.properties}")
                print(f"Payload: {msg.decode_content}")
                print("-" * 60)
    except KeyboardInterrupt:
        print("\nStopped listener.")
    except Exception as e:
        print(f"Error during listening: {e}")

def main() -> NoReturn:
    parser = argparse.ArgumentParser(description="Pulsar Live CLI Tool")

    # Global Arguments (Optional overrides)
    parser.add_argument("--broker", help="Override Broker URL")
    parser.add_argument("--admin", help="Override Admin URL")
    parser.add_argument("--token", help="Override Auth Token")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommands
    subparsers.add_parser("tenants", help="List all tenants")

    topic_parser = subparsers.add_parser("topics", help="List topics")
    topic_parser.add_argument("namespace", nargs="?", default="public/default", help="Namespace")

    listen_parser = subparsers.add_parser("listen", help="Listen to a topic stream")
    listen_parser.add_argument("topic", help="Full topic name")
    listen_parser.add_argument("--limit", type=int, default=20, help="Max messages")

    args = parser.parse_args()

    # Initialize Logic with Pydantic + CLI config
    try:
        config = get_config(args)
        # Simple feedback to know where we are connecting
        print(f"DEBUG: Connecting Admin: {config['admin_service_url']}")

        manager = PulsarManager(config)
    except Exception as e:
        print(f"Configuration Error: {e}")
        sys.exit(1)

    try:
        match args.command:
            case "tenants":
                cmd_list_tenants(manager)
            case "topics":
                cmd_list_topics(manager, args)
            case "listen":
                cmd_listen(manager, args)
            case _:
                parser.print_help()
    finally:
        if 'manager' in locals():
            manager.close()

    sys.exit(0)

if __name__ == "__main__":
    main()