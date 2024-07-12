import dotenv


class Settings:
    def __init__(self):
        env = self._read_env()
        self.DB_USER: str = env.get("USER")
        self.DB_PASSWORD: str = env.get("PASSWORD")
        self.DB_NAME: str = env.get("DB_NAME")
        self.DB_HOST: str = env.get("HOST")
        self.BATCH_SIZE = env.get("BATCH_SIZE")

    def _read_env(self):
        return dotenv.dotenv_values(".env")

    def get_batch_size(self):
        return self.BATCH_SIZE

    def get_url(self):
        return f"postgresql+psycopg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:5432/{self.DB_NAME}"


settings = Settings()
