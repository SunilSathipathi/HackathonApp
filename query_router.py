import logging
from typing import Literal, Dict, Any
from openai import OpenAI
from config import settings

logger = logging.getLogger(__name__)


class QueryRouter:
    """Classifies user questions into sql, semantic, or hybrid using GPT."""

    def __init__(self):
        self.client = OpenAI(api_key=settings.openai_api_key)
        self.model = settings.openai_model

    def classify(self, question: str, schema_summary: str) -> Dict[str, Any]:
        """
        Returns a dict: { type: 'sql'|'semantic'|'hybrid', reason: str }
        """
        prompt = (
            "You are a router deciding how to answer a question.\n"
            "Options: sql, semantic, hybrid.\n"
            "Prefer SQL for structured, exact lookups (counts, lists, filters, joins).\n"
            "Use semantic only when no clear structured path exists and fuzzy text meaning is required.\n"
            "Use hybrid when both structured filters and fuzzy matching help.\n"
            "Default to sql for aggregates (COUNT), lookups by known columns, and relational joins.\n"
            "Schema: \n" + schema_summary + "\n"
            "Respond with JSON: {\"type\": <sql|semantic|hybrid>, \"reason\": <short>}"
        )

        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Classify the question routing type succinctly."},
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.0,
            )
            import json
            return json.loads(resp.choices[0].message.content)
        except Exception as e:
            logger.error(f"Router classification error: {e}")
            return {"type": "sql", "reason": "fallback"}