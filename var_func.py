import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql

import io
import os

from timeit import default_timer as timer

import plotly.express as px

from jupyter_dash import JupyterDash

import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output


conn_params = {
    'host':'localhost',
    'user':'postgres',
    'password':'root',
    'database':'irio_2016'
}

def get_connection():
    global conn_params
    try :
        conn = psycopg2.connect(**conn_params)
        return conn
    except Exception as e:
        print(e)

conn = get_connection()

globals()['tables'] = {

    'kode_provinsi':{
        'name':'kode_provinsi',
        'attr':{
            'kode':'kode',
            'nama':'nama'
        },
    },
    'kode_industri':{
        'name':'kode_industri',
        'attr':{
            'kode':'kode',
            'nama':'nama'
        }
    },
    'kode_pulau':{
        'name':'kode_pulau',
        'attr':{
            'kode':'kode',
            'nama':'nama'
        }
    },
    'kode_lapus':{
        'name':'kode_lapus',
        'attr':{
            'kode':'kode',
            'nama':'nama'
        }
    },
    'kode_pengeluaran':{
        'name':'kode_pengeluaran',
        'attr':{
            'kode':'kode',
            'nama':'nama'
        }
    },
    'kode_pendapatan':{
        'name':'kode_pendapatan',
        'attr':{
            'kode':'kode',
            'nama':'nama'
        }
    },
    'rel_lapus_industri':{
        'name':'rel_lapus_industri',
        'attr':{
            'kode_industri':'kode_industri',
            'kode_lapus':'kode_lapus'
        }
    },
    'rel_pulau_provinsi':{
        'name':'rel_pulau_provinsi',
        'attr':{
            'kode_pulau':'kode_pulau',
            'kode_provinsi':'kode_provinsi'
        }
    },
    'input_antara_nasional_industri':{
        'name':'input_antara_nasional_industri',
        'attr':{
            'industri_penyedia':'industri_penyedia',
            'industri_pengguna':'industri_pengguna',
            'nilai_juta':'nilai_juta'
        }
    },
    'input_antara_nasional_lapus':{
        'name':'input_antara_nasional_lapus',
        'attr':{
            'lapus_penyedia':'lapangan_usaha_penyedia',
            'lapus_pengguna':'lapangan_usaha_pengguna',
            'nilai_juta':'nilai_juta'
        }
    },
    'input_antara_nasional_provinsi':{
        'name':'input_antara_nasional_provinsi',
        'attr':{
            'provinsi_penyedia':'provinsi_penyedia',
            'provinsi_pengguna':'provinsi_pengguna',
            'nilai_juta':'nilai_juta'
        }
    },
    'input_antara_nasional_pulau':{
        'name':'input_antara_nasional_pulau',
        'attr':{
            'pulau_penyedia':'pulau_penyedia',
            'pulau_pengguna':'pulau_pengguna',
            'nilai_juta':'nilai_juta'
        }
    },
    'input_antara_nasional_provinsi_industri':{
        'name':'input_antara_nasional_provinsi_industri',
        'attr':{
            'provinsi_penyedia':'provinsi_penyedia',
            'industri_penyedia':'industri_penyedia',
            'provinsi_pengguna':'provinsi_pengguna',
            'industri_pengguna':'industri_pengguna',
            'nilai_juta':'nilai_juta'
        }
    },
    'input_antara_nasional_provinsi_lapus':{
        'name':'input_antara_nasional_provinsi_lapus',
        'attr':{
            'provinsi_penyedia':'provinsi_penyedia',
            'lapus_penyedia':'lapangan_usaha_penyedia',
            'provinsi_pengguna':'provinsi_pengguna',
            'lapus_pengguna':'lapangan_usaha_pengguna',
            'nilai_juta':'nilai_juta'
        }
    },
    'input_antara_nasional_pulau_industri':{
        'name':'input_antara_nasional_pulau_industri',
        'attr':{
            'pulau_penyedia':'pulau_penyedia',
            'industri_penyedia':'industri_penyedia',
            'pulau_pengguna':'pulau_pengguna',
            'industri_pengguna':'industri_pengguna',
            'nilai_juta':'nilai_juta'
        }
    },
    'input_antara_nasional_pulau_lapus':{
        'name':'input_antara_nasional_pulau_lapus',
        'attr':{
            'pulau_penyedia':'pulau_penyedia',
            'lapus_penyedia':'lapangan_usaha_penyedia',
            'pulau_pengguna':'pulau_pengguna',
            'lapus_pengguna':'lapangan_usaha_pengguna',
            'nilai_juta':'nilai_juta'
        }
    },
    'konsumsi_akhir_nasional':{
        'name':'konsumsi_akhir_nasional',
        'attr':{
            'provinsi_penyedia':'provinsi_penyedia',
            'industri_penyedia':'industri_penyedia',
            'provinsi_pengguna':'provinsi_pengguna',
            'komponen_pengeluaran':'komponen_pengeluaran',
            'nilai_juta':'nilai_juta'
        }
    },
    'pendapatan':{
        'name':'pendapatan',
        'attr':{
            'komponen_pendapatan':'komponen_pendapatan',
            'provinsi_pengguna':'provinsi_pengguna',
            'industri_pengguna':'industri_pengguna',
            'nilai_juta':'nilai_juta'
        }
    },
    'input_antara_impor':{
        'name':'input_antara_impor',
        'attr':{
            'provinsi_pengguna':'provinsi_pengguna',
            'industri_pengguna':'industri_pengguna',
            'nilai_juta':'nilai_juta'
        }
    },
    'konsumsi_akhir_impor':{
        'name':'konsumsi_akhir_impor',
        'attr':{
            'provinsi_pengguna':'provinsi_pengguna',
            'komponen_pengeluaran':'komponen_pengeluaran',
            'nilai_juta':'nilai_juta'
        }
    },
    'ekspor':{
        'name':'ekspor',
        'attr':{
            'kode_provinsi':'kode_provinsi',
            'kode_industri':'kode_industri',
            'nilai_juta':'nilai_juta'
        }
    },
    'total_input_antara_nasional':{
        'name':'total_input_antara_nasional',
        'attr':{
            'provinsi_pengguna':'provinsi_pengguna',
            'industri_pengguna':'industri_pengguna',
            'nilai_juta':'nilai_juta'
        }
    },
    'total_input_antara':{
        'name':'total_input_antara',
        'attr':{
            'provinsi_pengguna':'provinsi_pengguna',
            'industri_pengguna':'industri_pengguna',
            'nilai_juta':'nilai_juta'
        }
    },
    'total_permintaan_antara':{
        'name':'total_permintaan_antara',
        'attr':{
            'provinsi_pengguna':'provinsi_pengguna',
            'industri_pengguna':'industri_pengguna',
            'nilai_juta':'nilai_juta'
        }
    },
    'total_pendapatan':{
        'name':'total_pendapatan',
        'attr':{
            'provinsi_pengguna':'provinsi_pengguna',
            'industri_pengguna':'industri_pengguna',
            'nilai_juta':'nilai_juta'
        }
    },
    'total_io':{
        'name':'total_io',
        'attr':{
            'provinsi':'provinsi',
            'industri':'industri',
            'nilai_juta':'nilai_juta'
        }
    },
    'ki_provinsi_lapus':{
        'name':'ki_provinsi_lapus',
        'attr':{
            'provinsi_penyedia':'provinsi_penyedia',
            'lapus_penyedia':'lapus_penyedia',
            'provinsi_pengguna':'provinsi_pengguna',
            'lapus_pengguna':'lapus_pengguna',
            'nilai':'nilai'
        }
    },
    'ki_provinsi_industri':{
        'name':'ki_provinsi_industri',
        'attr':{
            'provinsi_penyedia':'provinsi_penyedia',
            'industri_penyedia':'industri_penyedia',
            'provinsi_pengguna':'provinsi_pengguna',
            'industri_pengguna':'industri_pengguna',
            'nilai':'nilai'
        }
    },
    'ki_pulau_industri':{
        'name':'ki_pulau_industri',
        'attr':{
            'pulau_penyedia':'pulau_penyedia',
            'industri_penyedia':'industri_penyedia',
            'pulau_pengguna':'pulau_pengguna',
            'industri_pengguna':'industri_pengguna',
            'nilai':'nilai'
        }
    },
    'ki_pulau_lapus':{
        'name':'ki_pulau_lapus',
        'attr':{
            'pulau_penyedia':'pulau_penyedia',
            'lapus_penyedia':'lapus_penyedia',
            'pulau_pengguna':'pulau_pengguna',
            'lapus_pengguna':'lapus_pengguna',
            'nilai':'nilai'
        }
    },
    'ki_industri':{
        'name':'ki_industri',
        'attr':{
            'industri_penyedia':'industri_penyedia',
            'industri_pengguna':'industri_pengguna',
            'nilai':'nilai'
        }
    },
    'ki_lapus':{
        'name':'ki_lapus',
        'attr':{
            'lapus_penyedia':'lapus_penyedia',
            'lapus_pengguna':'lapus_pengguna',
            'nilai':'nilai'
        }
    },
    'ki_pulau':{
        'name':'ki_pulau',
        'attr':{
            'pulau_penyedia':'pulau_penyedia',
            'pulau_pengguna':'pulau_pengguna',
            'nilai':'nilai'
        }
    },
    'ki_provinsi':{
        'name':'ki_provinsi',
        'attr':{
            'provinsi_penyedia':'provinsi_penyedia',
            'provinsi_pengguna':'provinsi_pengguna',
            'nilai':'nilai'
        }
    },
    'ko_provinsi_lapus':{
        'name':'ko_provinsi_lapus',
        'attr':{
            'provinsi_penyedia':'provinsi_penyedia',
            'lapus_penyedia':'lapus_penyedia',
            'provinsi_pengguna':'provinsi_pengguna',
            'lapus_pengguna':'lapus_pengguna',
            'nilai':'nilai'
        }
    },
    'ko_provinsi_industri':{
        'name':'ko_provinsi_industri',
        'attr':{
            'provinsi_penyedia':'provinsi_penyedia',
            'industri_penyedia':'industri_penyedia',
            'provinsi_pengguna':'provinsi_pengguna',
            'industri_pengguna':'industri_pengguna',
            'nilai':'nilai'
        }
    },
    'ko_pulau_industri':{
        'name':'ko_pulau_industri',
        'attr':{
            'pulau_penyedia':'pulau_penyedia',
            'industri_penyedia':'industri_penyedia',
            'pulau_pengguna':'pulau_pengguna',
            'industri_pengguna':'industri_pengguna',
            'nilai':'nilai'
        }
    },
    'ko_pulau_lapus':{
        'name':'ko_pulau_lapus',
        'attr':{
            'pulau_penyedia':'pulau_penyedia',
            'lapus_penyedia':'lapus_penyedia',
            'pulau_pengguna':'pulau_pengguna',
            'lapus_pengguna':'lapus_pengguna',
            'nilai':'nilai'
        }
    },
    'ko_industri':{
        'name':'ko_industri',
        'attr':{
            'industri_penyedia':'industri_penyedia',
            'industri_pengguna':'industri_pengguna',
            'nilai':'nilai'
        }
    },
    'ko_lapus':{
        'name':'ko_lapus',
        'attr':{
            'lapus_penyedia':'lapus_penyedia',
            'lapus_pengguna':'lapus_pengguna',
            'nilai':'nilai'
        }
    },
    'ko_pulau':{
        'name':'ko_pulau',
        'attr':{
            'pulau_penyedia':'pulau_penyedia',
            'pulau_pengguna':'pulau_pengguna',
            'nilai':'nilai'
        }
    },
    'ko_provinsi':{
        'name':'ko_provinsi',
        'attr':{
            'provinsi_penyedia':'provinsi_penyedia',
            'provinsi_pengguna':'provinsi_pengguna',
            'nilai':'nilai'
        }
    },
}

tables['kode_provinsi']['data'] = pd.read_sql(f'SELECT * FROM {tables["kode_provinsi"]["name"]}', conn)
tables['kode_pulau']['data'] = pd.read_sql(f'SELECT * FROM {tables["kode_pulau"]["name"]}', conn)
tables['kode_industri']['data'] = pd.read_sql(f'SELECT * FROM {tables["kode_industri"]["name"]}', conn)
tables['kode_lapus']['data'] = pd.read_sql(f'SELECT * FROM {tables["kode_lapus"]["name"]}', conn)
tables['kode_pengeluaran']['data'] = pd.read_sql(f'SELECT * FROM {tables["kode_pengeluaran"]["name"]}', conn)
tables['kode_pendapatan']['data'] = pd.read_sql(f'SELECT * FROM {tables["kode_pendapatan"]["name"]}', conn)
tables['rel_lapus_industri']['data'] = pd.read_sql(f'SELECT * FROM {tables["rel_lapus_industri"]["name"]}', conn)
tables['rel_pulau_provinsi']['data'] = pd.read_sql(f'SELECT * FROM {tables["rel_pulau_provinsi"]["name"]}', conn)

conn.close()