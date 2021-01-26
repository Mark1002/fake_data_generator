"""Execute generator."""
from .data_schema import Message

def main():
    """Main."""
    while True:
        try:
            message = Message().to_dict()
            print(message)
        except KeyboardInterrupt:
            print('exit')
            break

if __name__ == "__main__":
    main()
