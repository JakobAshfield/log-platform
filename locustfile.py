import random
from locust import HttpUser, task, between

class LogPlatformUser(HttpUser):
    # Simulates active users sending payloads every 100ms to 500ms
    wait_time = between(0.1, 0.5)

    def on_start(self):
        """
        Executes on virtual user initialization. 
        Creates a dedicated mock user and extracts a token.
        """
        # Generate a distinct identifier to prevent collision issues
        self.username = f"loadtest_{random.randint(1, 999999)}"
        password = "testpassword"
        
        # 1. Register Account
        register_payload = {"username": self.username, "password": password}
        self.client.post("/auth/register", json=register_payload)
        
        # 2. Authenticate Session
        login_response = self.client.post("/auth/login", json=register_payload)
        
        if login_response.status_code == 200:
            token_data = login_response.json()
            access_token = token_data["access_token"]
            # Expose standard dictionary formatting context to the user instance
            self.auth_headers = {"Authorization": f"Bearer {access_token}"}
        else:
            # Safe boundary definition if initialization paths encounter rate limits
            self.auth_headers = {}

    @task(3)
    def ingest_single_log(self):
        """Standard transactional database insert write path."""
        self.client.post("/logs/", json={
            "level": random.choice(["info", "warn", "error"]),
            "message": f"Load test log {random.randint(1, 10000)}"
        }, headers=self.auth_headers)

    @task(5)
    def ingest_batch(self):
        """High-volume decoupled Kafka worker enqueue path."""
        self.client.post("/logs/batch", json={
            "entries": [
                {
                    "level": random.choice(["info", "warn", "error"]),
                    "message": f"Batch load test log chunk item {i}"
                }
                for i in range(10)
            ]
        }, headers=self.auth_headers)

    @task(2)
    def get_logs(self):
        """Read path fetching active database records."""
        self.client.get("/logs/", headers=self.auth_headers)
