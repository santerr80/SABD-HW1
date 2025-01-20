import luigi
import requests
import os
import tarfile


class Download(luigi.Task):
    file_name = luigi.Parameter(default="GSE68849")
    output_dir = luigi.Parameter(default="./downloads")

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, f"{self.file_name}.tar"))

    def run(self):
        os.makedirs(self.output_dir, exist_ok=True)
        url = f'https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.file_name}&format=file'
        response = requests.get(url=url, timeout=50)
        response.raise_for_status()  # Проверка на ошибки
        with open(self.output().path, 'wb') as f:
            f.write(response.content)
        print(f"Загрузка завершена и сохранена в {self.output().path}")


class ExtractFiles(luigi.Task):
    file_name = luigi.Parameter(default="GSE68849")
    output_dir = luigi.Parameter(default="./downloads")

    def requires(self):
        return Download(file_name=self.file_name, output_dir=self.output_dir)

    def output(self):
        # Возвращаем один LocalTarget, который будет указывать на завершение задачи
        return luigi.LocalTarget(os.path.join(self.output_dir, f"{self.file_name}_extracted.done"))

    def run(self):
        tar_path = self.requires().output().path
        os.makedirs(self.output_dir, exist_ok=True)

        with tarfile.open(tar_path, 'r') as tar:
            members = tar.getnames()
            for member in members:
                # Извлекаем файл в соответствующую директорию
                tar.extract(member, path=self.output_dir)
                print(f"Файл {member} извлечен в {self.output_dir}")

        # Создаем файл, чтобы обозначить, что задача завершена
        with self.output().open('w') as f:
            f.write('Extraction complete.')

        print(f"Извлечение завершено. Все файлы сохранены в {self.output_dir}")


if __name__ == '__main__':
    luigi.run()