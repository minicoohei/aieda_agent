#!/usr/bin/env python3
"""
AIEDA Model Manager
Handles model selection and fallback logic for AIEDA Agent
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any
from enum import Enum
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskType(Enum):
    """Task types for model selection"""
    IMPLEMENTATION = "implementation"
    GENERAL = "general"
    ANALYSIS = "analysis"
    COMMUNICATION = "communication"


@dataclass
class ModelConfig:
    """Model configuration data class"""
    provider: str
    model_id: str
    description: str
    max_tokens: Optional[int] = None
    temperature: Optional[float] = None
    launch_mode: Optional[str] = None
    priority: Optional[int] = None


class AIEDAModelManager:
    """Manager for AIEDA model selection and fallback"""

    def __init__(self):
        self.primary_model = ModelConfig(
            provider="Anthropic",
            model_id="claude-opus-4-6",
            description="Claude Opus 4.6 Max - Primary model",
            max_tokens=4096,
            temperature=0.7
        )

        self.implementation_chain = [
            ModelConfig(
                provider="Claude Code",
                model_id="claude-code",
                description="Claude Code with YOLO mode",
                launch_mode="YOLO",
                priority=0
            ),
            ModelConfig(
                provider="Kimi Code",
                model_id="kimi-code",
                description="Kimi Code - First fallback",
                priority=1
            ),
            ModelConfig(
                provider="Kimi",
                model_id="kimi-2.5-thinking",
                description="Kimi 2.5 Thinking - Second fallback",
                priority=2
            ),
            ModelConfig(
                provider="Anthropic",
                model_id="claude-3-5-sonnet-20241022",
                description="Claude Sonnet 4.5 - Final fallback",
                priority=3
            )
        ]

    def get_model_for_task(self, task_type: TaskType) -> List[ModelConfig]:
        """
        Get appropriate models for a given task type

        Args:
            task_type: Type of task to perform

        Returns:
            List of models in priority order
        """
        if task_type == TaskType.IMPLEMENTATION:
            return self.implementation_chain
        else:
            # For general tasks, use primary model with fallback to implementation chain
            return [self.primary_model] + self.implementation_chain[2:]  # Skip code-specific tools

    def select_model(self, task_type: str, available_models: Optional[List[str]] = None) -> ModelConfig:
        """
        Select the best available model for a task

        Args:
            task_type: Type of task as string
            available_models: List of available model IDs (optional)

        Returns:
            Selected model configuration
        """
        try:
            task_enum = TaskType(task_type.lower())
        except ValueError:
            logger.warning(f"Unknown task type: {task_type}, defaulting to GENERAL")
            task_enum = TaskType.GENERAL

        candidates = self.get_model_for_task(task_enum)

        if available_models:
            # Filter to only available models
            for model in candidates:
                if model.model_id in available_models:
                    logger.info(f"Selected model: {model.model_id} for task: {task_type}")
                    return model

        # Return primary candidate if no filtering needed
        selected = candidates[0]
        logger.info(f"Selected model: {selected.model_id} for task: {task_type}")
        return selected

    def get_fallback_chain(self, current_model_id: str, task_type: str) -> List[ModelConfig]:
        """
        Get fallback models for a given model and task

        Args:
            current_model_id: Current model that failed
            task_type: Type of task

        Returns:
            List of fallback models
        """
        try:
            task_enum = TaskType(task_type.lower())
        except ValueError:
            task_enum = TaskType.GENERAL

        candidates = self.get_model_for_task(task_enum)

        # Find current model index
        current_index = -1
        for i, model in enumerate(candidates):
            if model.model_id == current_model_id:
                current_index = i
                break

        # Return remaining models after current
        if current_index >= 0 and current_index < len(candidates) - 1:
            fallbacks = candidates[current_index + 1:]
            logger.info(f"Fallback chain for {current_model_id}: {[m.model_id for m in fallbacks]}")
            return fallbacks

        logger.warning(f"No fallbacks available for model: {current_model_id}")
        return []

    def export_config(self) -> Dict[str, Any]:
        """Export configuration as dictionary"""
        return {
            "primary_model": {
                "provider": self.primary_model.provider,
                "model_id": self.primary_model.model_id,
                "description": self.primary_model.description,
                "max_tokens": self.primary_model.max_tokens,
                "temperature": self.primary_model.temperature
            },
            "implementation_models": {
                "chain": [
                    {
                        "provider": model.provider,
                        "model_id": model.model_id,
                        "description": model.description,
                        "launch_mode": model.launch_mode,
                        "priority": model.priority
                    }
                    for model in self.implementation_chain
                ]
            }
        }


def main():
    """Main function for testing"""
    manager = AIEDAModelManager()

    # Test model selection
    print("=== Model Selection Tests ===")
    for task in ["implementation", "general", "analysis"]:
        model = manager.select_model(task)
        print(f"\nTask: {task}")
        print(f"Selected: {model.model_id} ({model.provider})")
        print(f"Description: {model.description}")

    # Test fallback chains
    print("\n=== Fallback Chain Tests ===")
    fallbacks = manager.get_fallback_chain("claude-code", "implementation")
    print(f"\nFallbacks for claude-code:")
    for fb in fallbacks:
        print(f"  - {fb.model_id} ({fb.provider})")

    # Export configuration
    print("\n=== Configuration Export ===")
    config = manager.export_config()
    print(json.dumps(config, indent=2))


if __name__ == "__main__":
    main()