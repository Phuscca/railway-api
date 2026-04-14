from app.schemas.seller import CalculateRequest


def notary_fee(value: float) -> float:
    if value < 50_000_000:
        return 50_000
    if value <= 100_000_000:
        return 100_000
    if value <= 1_000_000_000:
        return value * 0.001
    if value <= 3_000_000_000:
        return 1_000_000 + (value - 1_000_000_000) * 0.0006
    if value <= 5_000_000_000:
        return 2_200_000 + (value - 3_000_000_000) * 0.0005
    if value <= 10_000_000_000:
        return 3_200_000 + (value - 5_000_000_000) * 0.0004
    if value <= 100_000_000_000:
        return 5_200_000 + (value - 10_000_000_000) * 0.0003
    return min(70_000_000, 32_200_000 + (value - 100_000_000_000) * 0.0002)


def brokerage_fee(payload: CalculateRequest, sale_price: float | None = None) -> float:
    price = sale_price or payload.expected_sale_price
    if payload.brokerage_mode == 'fixed':
        return payload.brokerage_value
    return price * (payload.brokerage_value / 100)


def calculate_net_proceeds(payload: CalculateRequest, sale_price: float | None = None):
    price = sale_price or payload.expected_sale_price
    pit_tax = price * 0.02
    broker = brokerage_fee(payload, price)
    notary = notary_fee(price)
    net = price - pit_tax - broker - notary - payload.outstanding_loan
    return {
        'expected_sale_price': round(price),
        'pit_tax': round(pit_tax),
        'brokerage_fee': round(broker),
        'notary_fee': round(notary),
        'other_costs': 0,
        'outstanding_loan': round(payload.outstanding_loan),
        'estimated_net_proceeds': round(net),
    }


def build_scenarios(payload: CalculateRequest):
    prices = [
        ('ban_nhanh', payload.expected_sale_price * 0.98),
        ('can_bang', payload.expected_sale_price),
        ('toi_uu', payload.expected_sale_price * 1.03),
    ]
    return [
        {
            'label': label,
            'sale_price': round(price),
            'estimated_net_proceeds': calculate_net_proceeds(payload, price)['estimated_net_proceeds'],
        }
        for label, price in prices
    ]
