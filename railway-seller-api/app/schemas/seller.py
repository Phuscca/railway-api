from pydantic import BaseModel, Field


class CalculateRequest(BaseModel):
    project_name: str = Field(min_length=1)
    district: str = Field(min_length=1)
    area_net: float = Field(gt=0)
    bedrooms: int = Field(ge=0)
    expected_sale_price: float = Field(gt=0)
    outstanding_loan: float = Field(ge=0, default=0)
    brokerage_mode: str = 'percent'
    brokerage_value: float = Field(ge=0, default=1)
    target_net_proceeds: float = Field(ge=0, default=0)
    utm_source: str | None = None
    page_url: str | None = None


class TelegramLinkRequest(BaseModel):
    session_id: str
