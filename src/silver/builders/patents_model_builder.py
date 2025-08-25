from src.common.enums.model_type import ModelType
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.silver.builders.silver_model_builder import SilverModelBuilder


@ModelBuilderRegistry.register(ModelType.PATENTS)
class PatentsModelBuilder(SilverModelBuilder):
    def build(self, datasets):
        return super().build(datasets)