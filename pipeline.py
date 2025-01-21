import luigi
import os
import requests
import tarfile
import gzip
import shutil
from pathlib import Path
import io
import pandas as pd
import glob

class Download(luigi.Task):
    file_name = luigi.Parameter(default="GSE68849")
    output_dir = luigi.Parameter(default="./downloads")

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, f"{self.file_name}.tar"))

    def run(self):
        os.makedirs(self.output_dir, exist_ok=True)
        url = f'https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.file_name}&format=file'
        response = requests.get(url=url, timeout=50)
        response.raise_for_status()

        with open(self.output().path, 'wb') as f:
            f.write(response.content)
        print(f"Загрузка завершена и сохранена в {self.output().path}")

        if not os.path.exists(self.output().path):
            raise FileNotFoundError(f"Файл {self.output().path} не был создан.")


class ExtractTar(luigi.Task):
    file_name = luigi.Parameter(default="GSE68849")
    output_dir = luigi.Parameter(default="./extract")
    
    def requires(self):
        return Download(file_name=self.file_name)
    
    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, f"{self.file_name}_tar_extracted.done"))
    
    def run(self):
        os.makedirs(self.output_dir, exist_ok=True)
        
        with tarfile.open(self.requires().output().path, 'r') as tar:
            members = tar.getmembers()
            
            for i, member in enumerate(members, 1):
                subdir = os.path.join(self.output_dir, f"data{i}")
                os.makedirs(subdir, exist_ok=True)
                
                member.name = os.path.basename(member.name)
                tar.extract(member, path=subdir)
                print(f"Файл {member.name} распакован в {subdir}")
        
        Path(self.output().path).touch()
        print(f"Распаковка TAR завершена в {self.output_dir}")


class ExtractGz(luigi.Task):
    file_name = luigi.Parameter(default="GSE68849")
    output_dir = luigi.Parameter(default="./extract")
    
    def requires(self):
        return ExtractTar(file_name=self.file_name)
    
    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, f"{self.file_name}_gz_extracted.done"))
    
    def run(self):
        # Проходим по всем поддиректориям data1, data2, ...
        for subdir in Path(self.output_dir).glob('data*'):
            # Ищем все .gz файлы в текущей поддиректории
            for gz_file in subdir.glob('*.gz'):
                output_file = gz_file.with_suffix('')  # Убираем расширение .gz
                
                # Распаковываем gz файл
                with gzip.open(gz_file, 'rb') as f_in:
                    with open(output_file, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                # Удаляем исходный gz файл после распаковки
                gz_file.unlink()
                print(f"Файл {gz_file.name} распакован в {output_file.name}")
        
        Path(self.output().path).touch()
        print(f"Распаковка GZ файлов завершена")


class ProcessTxtFiles(luigi.Task):
    file_name = luigi.Parameter(default="GSE68849")
    input_dir = luigi.Parameter(default='./extract')
    output_dir = luigi.Parameter(default='./processed')

    def requires(self):
        return ExtractGz(file_name=self.file_name)

    def output(self):
        # Создаем маркерный файл для отслеживания выполнения
        return luigi.LocalTarget(os.path.join(self.output_dir, 'log'))

    def run(self):
        # Создаем выходную директорию если её нет
        os.makedirs(self.output_dir, exist_ok=True)

        # Ищем все txt файлы рекурсивно
        for txt_file in Path(self.input_dir).rglob('*.txt'):
            self.process_file(txt_file)

        # Создаем маркерный файл
        with self.output().open('w') as f:
            f.write('Все файлы обработаны')

    def process_file(self, file_path):
        dfs = {}
        with open(file_path) as f:
            write_key = None
            fio = io.StringIO()
            for l in f.readlines():
                if l.startswith('['):
                    if write_key:
                        fio.seek(0)
                        header = None if write_key == 'Heading' else 'infer'
                        dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                    fio = io.StringIO()
                    write_key = l.strip('[]\n')
                    continue
                if write_key:
                    fio.write(l)
            fio.seek(0)
            dfs[write_key] = pd.read_csv(fio, sep='\t')

        # Сохраняем каждый датафрейм в отдельный tsv файл
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        for df_name, df in dfs.items():
            output_file = os.path.join(
                self.output_dir, 
                f"{base_name}_{df_name}.tsv"
            )
            df.to_csv(output_file, sep='\t', index=False)

if __name__ == '__main__':
    luigi.build([ProcessTxtFiles()], local_scheduler=True)