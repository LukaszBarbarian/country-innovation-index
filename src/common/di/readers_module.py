# src/silver/di_modules.py
from injector import Module, singleton, Binder, provider
from src.common.enums.domain_source import DomainSource
from src.common.factories.data_reader_factory import DataReaderFactory
from src.common.di.di_module import DIModule



class ReadersModule(DIModule):
    def configure(self, binder: Binder) -> None:
        for domain_source in DomainSource:
            try:
                builder_class = DataReaderFactory.get_class(domain_source)

                binder.bind(builder_class, to=builder_class, scope=singleton)
            
            except KeyError:
                print(f"Ostrzeżenie: Brak readera '{domain_source.name}'.")
                continue
    