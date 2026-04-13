from pydantic import BaseModel, Field


class EventTrackRequest(BaseModel):
    event_type: str = Field(min_length=1)
    session_id: str | None = None
    event_value: str | None = None
    meta: dict | None = None


class TelegramConfirmRequest(BaseModel):
    link_token: str = Field(min_length=1)
    telegram_chat_id: str = Field(min_length=1)
    telegram_username: str | None = None
