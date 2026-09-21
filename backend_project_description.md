# Backend Project I Worked On: Real-Time Order Notification System

## Problem Statement
I had a real challenge - users were constantly refreshing their screens to see if orders had been updated. It was inefficient and created a terrible user experience. The main issue was that whenever someone created, updated, or deleted an order in the database, other users had no way of knowing about it immediately. I needed to build a way to push these updates to everyone in real-time without making clients constantly poll the server.

## My Exact Responsibility
I was the lead backend developer on this project, so I handled most of the core backend work:

- **FastAPI Development**: I built out all the REST API endpoints for managing orders - creating, reading, updating, and deleting them. I also implemented the API key authentication system to secure these endpoints.

- **WebSocket Implementation**: This was probably the most interesting part. I created the WebSocket manager that handles client connections and broadcasts notifications whenever something changes. Getting the connection management right and ensuring messages go to the right clients was tricky but rewarding.

- **Database Design**: I designed the notification queue system in MySQL that stores events durably. This was crucial for reliability - if the WebSocket connection drops, I don't lose any notifications.

- **Security**: I implemented all the security measures - API key validation, CORS policies, HTTPS requirements, and rate limiting to prevent abuse.

- **Monitoring**: I added health checks and structured logging so I could easily monitor the system and debug issues in production.

## Tech Stack I Used
- **Backend Framework**: FastAPI with Python - it was perfect for building APIs quickly with great documentation
- **Database**: MySQL with connection pooling for handling multiple concurrent requests
- **Message Queue**: Apache Kafka for streaming the change events
- **CDC Tool**: Debezium to capture database changes automatically
- **Real-time Communication**: WebSockets for pushing updates to clients
- **Containerization**: Docker Compose for running all the services locally
- **Security**: API key-based authentication system
- **Monitoring**: Structured JSON logging with correlation IDs for tracking requests

The whole system works as a pipeline: when something changes in MySQL, Debezium captures it, sends it to Kafka, my consumer worker processes it and stores it in a notification queue, then FastAPI reads from that queue and pushes updates to connected WebSocket clients. It's been really reliable and the users love getting instant updates instead of having to refresh constantly.
