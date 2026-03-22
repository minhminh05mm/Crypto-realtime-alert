from __future__ import annotations

from dataclasses import dataclass
from threading import Lock

import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

try:
    from src.config import get_logger
except ModuleNotFoundError:
    from config import get_logger


LOGGER = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class SentimentPrediction:
    label: str
    sentiment_score: float


class FinBertSentimentAnalyzer:
    _instance: "FinBertSentimentAnalyzer | None" = None
    _lock = Lock()

    def __init__(self, model_name: str, max_length: int) -> None:
        self.model_name = model_name
        self.max_length = max_length
        self.device = torch.device("cpu")
        self.tokenizer = self._load_tokenizer()
        self.model = self._load_model()
        self.label_to_index = {
            str(label).lower(): int(index)
            for index, label in self.model.config.id2label.items()
        }
        self.model.eval()
        self.model.to(self.device)
        LOGGER.info("FinBERT model loaded successfully model=%s", self.model_name)

    @classmethod
    def get_instance(
        cls, model_name: str, max_length: int
    ) -> "FinBertSentimentAnalyzer":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(model_name=model_name, max_length=max_length)

        return cls._instance

    def _load_tokenizer(self):
        try:
            return AutoTokenizer.from_pretrained(
                self.model_name,
                local_files_only=True,
            )
        except Exception:
            LOGGER.info(
                "FinBERT tokenizer not found in local cache. Downloading model=%s",
                self.model_name,
            )
            return AutoTokenizer.from_pretrained(self.model_name)

    def _load_model(self):
        try:
            return AutoModelForSequenceClassification.from_pretrained(
                self.model_name,
                local_files_only=True,
            )
        except Exception:
            LOGGER.info(
                "FinBERT weights not found in local cache. Downloading model=%s",
                self.model_name,
            )
            return AutoModelForSequenceClassification.from_pretrained(self.model_name)

    def predict(self, text: str | None) -> SentimentPrediction:
        if not text or not text.strip():
            return SentimentPrediction(label="Neutral", sentiment_score=0.0)

        try:
            encoded = self.tokenizer(
                text.strip(),
                max_length=self.max_length,
                padding="max_length",
                truncation=True,
                return_tensors="pt",
            )
            encoded = {key: value.to(self.device) for key, value in encoded.items()}

            with torch.no_grad():
                logits = self.model(**encoded).logits.squeeze(0)
                probabilities = torch.softmax(logits, dim=-1)

            label_index = int(torch.argmax(probabilities).item())
            label = self._normalize_label(self.model.config.id2label[label_index])
            positive_score = float(probabilities[self._find_label_index("positive")].item())
            negative_score = float(probabilities[self._find_label_index("negative")].item())
            sentiment_score = positive_score - negative_score

            return SentimentPrediction(label=label, sentiment_score=sentiment_score)
        except Exception:
            LOGGER.exception("FinBERT inference failed. Falling back to Neutral sentiment.")
            return SentimentPrediction(label="Neutral", sentiment_score=0.0)

    def _find_label_index(self, label_name: str) -> int:
        normalized_label = label_name.lower()
        if normalized_label not in self.label_to_index:
            raise ValueError(f"Unable to find label '{label_name}' in FinBERT config.")

        return self.label_to_index[normalized_label]

    @staticmethod
    def _normalize_label(label: str) -> str:
        normalized = label.strip().lower()
        if normalized == "positive":
            return "Positive"
        if normalized == "negative":
            return "Negative"
        return "Neutral"
