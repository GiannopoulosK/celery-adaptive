"""Core public API for django-smart-queue."""

from .decorators import adaptive_task

__all__ = ["adaptive_task"]
