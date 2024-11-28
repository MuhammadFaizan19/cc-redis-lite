from app.server import Server

def main():
    redis_server = Server()
    redis_server.start_server()

if __name__ == "__main__":
    main()
