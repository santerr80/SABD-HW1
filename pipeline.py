import luigi
import os
import requests
import tarfile
import gzip
import shutil
from pathlib import Path

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

if __name__ == '__main__':
    luigi.build([ExtractGz()], local_scheduler=True)