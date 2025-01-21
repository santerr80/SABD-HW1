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


class ProcessTextFiles(luigi.Task):
    file_name = luigi.Parameter(default="GSE68849")
    input_dir = luigi.Parameter(default='./extract')
    output_dir = luigi.Parameter(default='./processed')

    def requires(self):
        return ExtractGz(file_name=self.file_name)

    def output(self):
        # Создаем выходной каталог, если он не существует
        os.makedirs(self.output_dir, exist_ok=True)
        # Возвращаем список выходных файлов
        return [luigi.LocalTarget(os.path.join(self.output_dir, f"processed_{os.path.basename(f)}")) 
                for f in glob.glob(f"{self.input_dir}/**/*.txt", recursive=True)]

    def process_file(self, filepath):
        dfs = {}
        with open(filepath) as f:
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
        return dfs

    def run(self):
        # Получаем список всех txt файлов в каталоге и подкаталогах
        txt_files = glob.glob(f"{self.input_dir}/**/*.txt", recursive=True)
        
        for input_file in txt_files:
            # Обрабатываем файл
            processed_dfs = self.process_file(input_file)
            
            # Создаем имя выходного файла
            output_filename = os.path.join(self.output_dir, f"processed_{os.path.basename(input_file)}")
            
            # Сохраняем результаты
            with open(output_filename, 'w') as f:
                for key, df in processed_dfs.items():
                    f.write(f'[{key}]\n')
                    df.to_csv(f, sep='\t', index=False)

if __name__ == '__main__':
    luigi.build([ProcessTextFiles()], local_scheduler=True)