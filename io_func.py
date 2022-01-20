from var_func import *

# MENGAMBIL KOEFISIEN MATRIKS INPUT / OUTPUT
def get_coef_matrix(by, aggregate):
    '''
    aggregate : ['input', 'output']
    '''
    row_col = {
        'industri':52, 'pulau':6, 'lapus':17, 'provinsi':34, 'provinsi_industri':34*52, 
        'provinsi_lapus':34*17, 'pulau_industri':6*52, 'pulau_lapus':6*17
    }
    
    if aggregate not in ['input', 'output']:
        raise Exception("Argumen aggregate harus diantara nilai berikut : ['input', 'output']")
    
    if by not in row_col.keys():
        raise Exception("Argumen by harus diantara nilai berikut : ['industri', 'pulau', 'lapus', 'provinsi', 'industri_provinsi', 'industri_pulau', 'lapus_provinsi', 'lapus_pulau']")
    
    
    table_name,nilai,shape = (tables[f'ki_{by}']['name'],tables[f'ki_{by}']['attr']['nilai'],row_col[by]) if aggregate == 'input' else (tables[f'ko_{by}']['name'],tables[f'ko_{by}']['attr']['nilai'],row_col[by])
    
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f'''SELECT {nilai} FROM {table_name}''')
    mat = np.array(cur.fetchall()).reshape(shape,shape)
    cur.close()
    conn.close()
    return mat
    
    
# MENGUBAH KODE MENJADI NAMA
def change_code(data, columns_reference):
    '''
    data : dataframe
    columns_reference : dict. Contoh {'kode_provinsi':'provinsi'}
    '''
    data_copied = data.copy()
    for column, reference in columns_reference.items():
        data_copied = pd.concat([
            data_copied.set_index(column),
            tables[f'kode_{reference}']['data'].set_index(tables[f'kode_{reference}']['attr']['kode'])
        ], axis = 1, join = "inner").rename(columns = {tables[f'kode_{reference}']['attr']['nama']:column})
    
    return data_copied.reset_index(drop = True)




# MENGAMBIL DATA TOTAL INPUT-OUTPUT
def sql_total_io(unit):
    total_io = tables["total_io"]["name"]
    nilai_juta = tables["total_io"]["attr"]["nilai_juta"]

    if unit == 'provinsi_industri':
        return f'SELECT * FROM {total_io}'
    elif unit in ['industri', 'provinsi']:
        return f'''
        SELECT 
        {unit},
        sum({nilai_juta}) as nilai_juta
        FROM {total_io}
        GROUP BY {unit}
        '''
    elif unit in ['pulau','lapus']:
        rel_table = tables['rel_lapus_industri']['name'] if unit == 'lapus' else tables['rel_pulau_provinsi']['name']
        forkey_column = tables['total_io']['attr']['industri'] if unit == 'lapus' else tables['total_io']['attr']['provinsi']
        refer_column = tables['rel_lapus_industri']['attr']['kode_industri'] if unit == 'lapus' else tables['rel_pulau_provinsi']['attr']['kode_provinsi']
        show_refer_column, show_column = (tables['rel_lapus_industri']['attr']['kode_lapus'], 'lapangan_usaha') if unit == 'lapus' else (tables['rel_pulau_provinsi']['attr']['kode_pulau'], 'pulau')

        return f'''
        SELECT 
        b.{show_refer_column} AS {show_column},
        SUM(a.{nilai_juta}) AS nilai_juta
        FROM {total_io} a
        LEFT JOIN {rel_table} b
        ON b.{refer_column} = a.{forkey_column}
        GROUP BY b.{show_refer_column}
        '''

    elif unit in ['provinsi_lapus', 'pulau_industri', 'pulau_lapus']:
        column_name = {'provinsi':'provinsi', 'industri':'industri', 'pulau':'pulau', 'lapus':'lapangan_usaha'}
        column_selected, table_joined = [], ['']

        for i in unit.split('_'):
            if i == 'pulau':
                column_selected.append(f'b1.{tables["rel_pulau_provinsi"]["attr"]["kode_pulau"]} AS pulau')
                table_joined.append(f'''
                {tables["rel_pulau_provinsi"]["name"]} AS b1
                ON b1.{tables["rel_pulau_provinsi"]["attr"]["kode_provinsi"]} = a.{tables["total_io"]["attr"]["provinsi"]}
                ''')
            if i == 'lapus':
                column_selected.append(f'b2.{tables["rel_lapus_industri"]["attr"]["kode_lapus"]} AS lapangan_usaha')
                table_joined.append(f'''
                {tables["rel_lapus_industri"]["name"]} AS b2
                ON b2.{tables["rel_lapus_industri"]["attr"]["kode_industri"]} = a.{tables["total_io"]["attr"]["industri"]}
                ''')
            if i not in ['pulau','lapus']:
                column_selected.append(f'a.{tables["total_io"]["attr"][i]}')
        column_selected.append(f'SUM(a.{nilai_juta}) AS nilai_juta')

        return f'''
        SELECT 
        {", ".join(column_selected)}
        FROM {total_io} AS a
        {' LEFT JOIN '.join(table_joined)}
        GROUP BY 
        {", ".join([column_name[i] for i in unit.split('_')])}
        '''
        
        
        
        

# MENGAMBIL DATA INPUT/PERMINTAAN ANTARA NASIONAL
def sql_ia_per_unit(from_, to_):
    
    if from_ == to_:
        return f'SELECT * FROM {tables[f"input_antara_nasional_{from_}"]["name"]}'
    else:
        column_name = {'provinsi':'provinsi', 'industri':'industri', 'pulau':'pulau', 'lapus':'lapangan_usaha'}
        column_selected = []
        table_joined = ['']
        if from_ in ['industri','provinsi']:
            column_selected.append(f'a.{tables["input_antara_nasional_provinsi_industri"]["attr"][f"{from_}_penyedia"]}')
            
            if to_ in ['industri', 'provinsi']:
                column_selected.append(f'a.{tables["input_antara_nasional_provinsi_industri"]["attr"][f"{to_}_pengguna"]}')
        
            elif to_ in ['pulau', 'lapus']:
                column_selected.append(f'''{'b1' if to_ == 'pulau' else 'b2'}.{tables[f"rel_{to_}_{'provinsi' if to_ == 'pulau' else 'industri'}"]["attr"][f"kode_{to_}"]} AS {'pulau' if to_ == 'pulau' else 'lapangan_usaha'}_pengguna''')
                table_joined.append(
                    f'''
                    {tables[f"rel_{to_}_{'provinsi' if to_ == 'pulau' else 'industri'}"]["name"]} AS {'b1' if to_ == 'pulau' else 'b2'}
                    ON {'b1' if to_ == 'pulau' else 'b2'}.{tables[f"rel_{to_}_{'provinsi' if to_ == 'pulau' else 'industri'}"]["attr"][f"kode_{'provinsi' if to_ == 'pulau' else 'industri'}"]} = a.{tables["input_antara_nasional_provinsi_industri"]["attr"][f"{'provinsi' if to_ == 'pulau' else 'industri'}_pengguna"]}
                    '''
                )
            elif to_ in ['provinsi_industri', 'provinsi_lapus', 'pulau_industri', 'pulau_lapus']:
                for unit in to_.split('_'):
                    if unit in ['pulau', 'lapus']:
                        column_selected.append(f'''{'c1' if unit == 'pulau' else 'c2'}.{tables[f"rel_{unit}_{'provinsi' if unit == 'pulau' else 'industri'}"]["attr"][f"kode_{unit}"]} AS {column_name[unit]}_pengguna''')
                        table_joined.append(
                            f'''
                            {tables[f"rel_{unit}_{'provinsi' if unit == 'pulau' else 'industri'}"]["name"]} AS {'c1' if unit == 'pulau' else 'c2'}
                            ON {'c1' if unit == 'pulau' else 'c2'}.{tables[f"rel_{unit}_{'provinsi' if unit == 'pulau' else 'industri'}"]["attr"][f"kode_{'provinsi' if unit == 'pulau' else 'industri'}"]} = a.{tables["input_antara_nasional_provinsi_industri"]["attr"][f"{'provinsi' if unit == 'pulau' else 'industri'}_pengguna"]}
                            '''
                        )
                    else:
                        column_selected.append(f'a.{unit}_pengguna')
                
        elif from_ in ['pulau', 'lapus']:
            column_selected.append(f'''{'b1' if from_ == 'pulau' else 'b2'}.{tables[f"rel_{from_}_{'provinsi' if from_ == 'pulau' else 'industri'}"]["attr"][f"kode_{from_}"]} AS {'pulau' if from_ == 'pulau' else 'lapangan_usaha'}_penyedia''')
            table_joined.append(
                f'''
                {tables[f"rel_{from_}_{'provinsi' if from_ == 'pulau' else 'industri'}"]["name"]} AS {'b1' if from_ == 'pulau' else 'b2'}
                ON {'b1' if from_ == 'pulau' else 'b2'}.{tables[f"rel_{from_}_{'provinsi' if from_ == 'pulau' else 'industri'}"]["attr"][f"kode_{'provinsi' if from_ == 'pulau' else 'industri'}"]} = a.{tables["input_antara_nasional_provinsi_industri"]["attr"][f"{'provinsi' if from_ == 'pulau' else 'industri'}_penyedia"]}
                '''
            )
            if to_ in ['industri', 'provinsi']:
                column_selected.append(f'a.{tables["input_antara_nasional_provinsi_industri"]["attr"][f"{to_}_pengguna"]}')
            elif to_ in ['pulau', 'lapus']:
                column_selected.append(f'''{'b1' if to_ == 'pulau' else 'b2'}.{tables[f"rel_{to_}_{'provinsi' if to_ == 'pulau' else 'industri'}"]["attr"][f"kode_{to_}"]} AS {'pulau' if to_ == 'pulau' else 'lapangan_usaha'}_pengguna''')
                table_joined.append(
                    f'''
                    {tables[f"rel_{to_}_{'provinsi' if to_ == 'pulau' else 'industri'}"]["name"]} AS {'b1' if to_ == 'pulau' else 'b2'}
                    ON {'b1' if to_ == 'pulau' else 'b2'}.{tables[f"rel_{to_}_{'provinsi' if to_ == 'pulau' else 'industri'}"]["attr"][f"kode_{'provinsi' if to_ == 'pulau' else 'industri'}"]} = a.{tables["input_antara_nasional_provinsi_industri"]["attr"][f"{'provinsi' if to_ == 'pulau' else 'industri'}_pengguna"]}
                    '''
                )
            elif to_ in ['provinsi_industri', 'provinsi_lapus', 'pulau_industri', 'pulau_lapus']:
                for unit in to_.split('_'):
                    if unit in ['pulau', 'lapus']:
                        column_selected.append(f'''{'c1' if unit == 'pulau' else 'c2'}.{tables[f"rel_{unit}_{'provinsi' if unit == 'pulau' else 'industri'}"]["attr"][f"kode_{unit}"]} AS {column_name[unit]}_pengguna''')
                        table_joined.append(
                            f'''
                            {tables[f"rel_{unit}_{'provinsi' if unit == 'pulau' else 'industri'}"]["name"]} AS {'c1' if unit == 'pulau' else 'c2'}
                            ON {'c1' if unit == 'pulau' else 'c2'}.{tables[f"rel_{unit}_{'provinsi' if unit == 'pulau' else 'industri'}"]["attr"][f"kode_{'provinsi' if unit == 'pulau' else 'industri'}"]} = a.{tables["input_antara_nasional_provinsi_industri"]["attr"][f"{'provinsi' if unit == 'pulau' else 'industri'}_pengguna"]}
                            '''
                        )
                    else:
                        column_selected.append(f'a.{unit}_pengguna')
                
        elif from_ in ['provinsi_industri', 'provinsi_lapus', 'pulau_industri', 'pulau_lapus']:
            for unit in from_.split('_'):
                if unit in ['pulau', 'lapus']:
                    column_selected.append(f'''{'b1' if unit == 'pulau' else 'b2'}.{tables[f"rel_{unit}_{'provinsi' if unit == 'pulau' else 'industri'}"]["attr"][f"kode_{unit}"]} AS {column_name[unit]}_penyedia''')
                    table_joined.append(
                        f'''
                        {tables[f"rel_{unit}_{'provinsi' if unit == 'pulau' else 'industri'}"]["name"]} AS {'b1' if unit == 'pulau' else 'b2'}
                        ON {'b1' if unit == 'pulau' else 'b2'}.{tables[f"rel_{unit}_{'provinsi' if unit == 'pulau' else 'industri'}"]["attr"][f"kode_{'provinsi' if unit == 'pulau' else 'industri'}"]} = a.{tables["input_antara_nasional_provinsi_industri"]["attr"][f"{'provinsi' if unit == 'pulau' else 'industri'}_penyedia"]}
                        '''
                    )
                else:
                    column_selected.append(f'a.{unit}_penyedia')
                
            if to_ in ['industri','provinsi']:
                column_selected.append(f'a.{tables["input_antara_nasional_provinsi_industri"]["attr"][f"{to_}_pengguna"]}')
            elif to_ in ['pulau', 'lapus']:
                column_selected.append(f'''{'c1' if to_ == 'pulau' else 'c2'}.{tables[f"rel_{to_}_{'provinsi' if to_ == 'pulau' else 'industri'}"]["attr"][f"kode_{to_}"]} AS {'pulau' if to_ == 'pulau' else 'lapangan_usaha'}_pengguna''')
                table_joined.append(
                    f'''
                    {tables[f"rel_{to_}_{'provinsi' if to_ == 'pulau' else 'industri'}"]["name"]} AS {'c1' if to_ == 'pulau' else 'c2'}
                    ON {'c1' if to_ == 'pulau' else 'c2'}.{tables[f"rel_{to_}_{'provinsi' if to_ == 'pulau' else 'industri'}"]["attr"][f"kode_{'provinsi' if to_ == 'pulau' else 'industri'}"]} = a.{tables["input_antara_nasional_provinsi_industri"]["attr"][f"{'provinsi' if to_ == 'pulau' else 'industri'}_pengguna"]}
                    '''
                )
            elif to_ in ['provinsi_industri', 'provinsi_lapus', 'pulau_industri', 'pulau_lapus']:
                for unit in to_.split('_'):
                    if unit in ['pulau', 'lapus']:
                        column_selected.append(f'''{'c1' if unit == 'pulau' else 'c2'}.{tables[f"rel_{unit}_{'provinsi' if unit == 'pulau' else 'industri'}"]["attr"][f"kode_{unit}"]} AS {column_name[unit]}_pengguna''')
                        table_joined.append(
                            f'''
                            {tables[f"rel_{unit}_{'provinsi' if unit == 'pulau' else 'industri'}"]["name"]} AS {'c1' if unit == 'pulau' else 'c2'}
                            ON {'c1' if unit == 'pulau' else 'c2'}.{tables[f"rel_{unit}_{'provinsi' if unit == 'pulau' else 'industri'}"]["attr"][f"kode_{'provinsi' if unit == 'pulau' else 'industri'}"]} = a.{tables["input_antara_nasional_provinsi_industri"]["attr"][f"{'provinsi' if unit == 'pulau' else 'industri'}_pengguna"]}
                            '''
                        )
                    else:
                        column_selected.append(f'a.{unit}_pengguna')
        
        groupby = [f"{column_name[i]}_penyedia" for i in from_.split('_')] + [f"{column_name[j]}_pengguna" for j in to_.split('_')]
        column_selected.append(f'SUM(a.{tables["input_antara_nasional_provinsi_industri"]["attr"]["nilai_juta"]}) AS nilai_juta')
        return f'''SELECT 
        {', '.join(column_selected)}
        FROM {tables["input_antara_nasional_provinsi_industri"]["name"]} a
        {' LEFT JOIN '.join(table_joined)}
        GROUP BY 
        {", ".join(groupby)}'''



# MENGAMBIL DATA PROPORSI INPUT-OUTPUT BERDASARKAN KONDISI PEMILIHAN
def io_used_per_unit(from_, to_, aggregate, condition):
    if from_ not in ['industri','pulau','provinsi','lapus','provinsi_industri','provinsi_lapus','pulau_industri','pulau_lapus']:
        raise Exception('1')
    
    if to_ not in ['industri','pulau','provinsi','lapus','provinsi_industri','provinsi_lapus','pulau_industri','pulau_lapus']:
        raise Exception('2')
    
    if (type(from_) is not str) or (type(to_) is not str):
        raise Exception('3')
    
#     if not ((set(from_.split('_')) <= set(condition.keys())) and (set(to_.split('_')) <= set(condition.keys()))):
#         raise Exception('4')
    
    if aggregate not in ['input','output']:
        raise Exception('5')
    column_name = {'provinsi':'provinsi', 'industri':'industri', 'pulau':'pulau', 'lapus':'lapangan_usaha'}
    column_selected = []
    on_ = [
        f'a.{column_name[from_unit]}_penyedia = b.{column_name[from_unit]}' for from_unit in from_.split('_')
    ] if aggregate == 'output' else [
        f'a.{column_name[to_unit]}_pengguna = b.{column_name[to_unit]}' for to_unit in to_.split('_')
    ]
    
    for from_unit in from_.split('_'):
        column_selected.append(f'a.{column_name[from_unit]}_penyedia')
    for to_unit in to_.split('_'):
        column_selected.append(f'a.{column_name[to_unit]}_pengguna')
    
    column_selected.append(f'a.nilai_juta/b.nilai_juta as nilai')
    
    return f'''
    WITH c AS (
        WITH b AS (
            {sql_total_io(from_) if aggregate == 'output' else sql_total_io(to_)}
        ), a AS (
            {sql_ia_per_unit(from_,to_)}
        )
        SELECT 
        {', '.join(column_selected)}
        FROM a
        LEFT JOIN b
        ON {' AND '.join(on_)}
    )
    SELECT * FROM c
    WHERE {' AND '.join([f'{key} = %({key})s' for key in condition.keys()])}
    ORDER BY nilai DESC
    '''
    







