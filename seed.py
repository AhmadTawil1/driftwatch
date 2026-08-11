"""Registers the providers, models, and starting prices this project
watches. Run with: uv run python seed.py

Safe to re-run: it checks for an existing row by name before inserting,
so it doesn't create duplicates.
"""

from datetime import date

from dotenv import load_dotenv

from watchdog.db.models import Model, ModelPrice, Provider
from watchdog.db.session import get_session

load_dotenv()

PROVIDERS = [
    {
        "name": "openai",
        "base_url": "https://api.openai.com/v1",
        "credential_env_var": "OPENAI_API_KEY",
        "concurrency_limit": 5,
    },
    {
        "name": "anthropic",
        "base_url": "https://api.anthropic.com/v1",
        "credential_env_var": "ANTHROPIC_API_KEY",
        "concurrency_limit": 5,
    },
]

# Prices are per million tokens, as advertised by each provider at the time
# of writing. They're a starting point, not a live feed — cost accounting
# code will use whichever price row's effective_from is most recent as of
# the call, so updating a price later is just adding a new row here.
MODELS = [
    {
        "provider": "openai",
        "name": "gpt-4o-mini",
        "input_price_per_million": "0.15",
        "output_price_per_million": "0.60",
    },
    {
        "provider": "openai",
        "name": "gpt-4o",
        "input_price_per_million": "2.50",
        "output_price_per_million": "10.00",
    },
    {
        "provider": "anthropic",
        "name": "claude-haiku-4-5",
        "input_price_per_million": "1.00",
        "output_price_per_million": "5.00",
    },
    {
        "provider": "anthropic",
        "name": "claude-sonnet-4-5",
        "input_price_per_million": "3.00",
        "output_price_per_million": "15.00",
    },
]


def main() -> None:
    session = get_session()
    today = date.today()

    providers_by_name: dict[str, Provider] = {}
    for row in PROVIDERS:
        provider = session.query(Provider).filter_by(name=row["name"]).one_or_none()
        if provider is None:
            provider = Provider(**row)
            session.add(provider)
            session.flush()
            print(f"created provider: {provider.name}")
        else:
            print(f"provider already exists: {provider.name}")
        providers_by_name[provider.name] = provider

    for row in MODELS:
        provider = providers_by_name[row["provider"]]
        model = (
            session.query(Model)
            .filter_by(provider_id=provider.id, name=row["name"])
            .one_or_none()
        )
        if model is None:
            model = Model(provider_id=provider.id, name=row["name"])
            session.add(model)
            session.flush()
            print(f"created model: {provider.name}/{model.name}")
        else:
            print(f"model already exists: {provider.name}/{model.name}")

        has_price_today = (
            session.query(ModelPrice)
            .filter_by(model_id=model.id, effective_from=today)
            .one_or_none()
        )
        if has_price_today is None:
            session.add(
                ModelPrice(
                    model_id=model.id,
                    input_price_per_million=row["input_price_per_million"],
                    output_price_per_million=row["output_price_per_million"],
                    effective_from=today,
                )
            )
            print(f"  priced as of {today}")

    session.commit()
    session.close()


if __name__ == "__main__":
    main()
