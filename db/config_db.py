import configparser
import pandas as pd

from pathlib import Path

class backconfig(object):

    def to_date(self, x):
        #переводим текстовый формат даты из конфиг файла в формат numpy data

        return pd.to_datetime(x)


    def __init__(self):

        self.db_conf_path = Path(Path(__file__) / '..' / '..' / 'config' / 'config_db.cf').resolve()

        self.db_conf_data = {
            'DEFAULT': {'host': 'localhost', 'dbname': 'demo', 'user': 'db_user'},
            'data_type': {'dbconfig': {'host': str, 'dbname': str, 'user': str, 'password':str}}
        }

        if not self.db_conf_path.exists():
            self._create_config_file(self.db_conf_data, self.db_conf_path)

        dbconf = self._filling_config_data(self.db_conf_data, self.db_conf_path)
        self.dsn = self._dsn(dbconf['dbconfig'])

    def _create_config_file(self, data, path):

        if not path.parent.exists():
            Path.mkdir(path.parent)
        config = configparser.ConfigParser()
        dt = data['data_type']
        for s in dt.keys():
            config.add_section(s)
            for k in dt[s].keys():
                config[s][k] = data['DEFAULT'][k]

        with open(path, 'w') as conf_file:
            config.write(conf_file)

    def _filling_config_data(self, data, path):

        config = configparser.ConfigParser()
        config.read(path)
        dt = data['data_type']

        for s in config.sections():
            for o, v in config.items(s):
                if not config[s][o]:
                    print(f'Заполните свойство {o} раздела [{s}] файла {path}')
                    exit()
        #На основании функций из словаря data_type
        conf_data = {s: {o: dt[s][o](v) for o, v in config.items(s)} for s in config.sections()}

        return conf_data

    def _dsn(self, confdata):
        result = ''
        for item in confdata.items():
            t = f'{item[0]}={item[1]} '
            result = f'{result} {t}'

        return result