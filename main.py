#importacion de librerias necesarias para trabajar en el proyecto
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

#creacion de la conexion a la base de datos
engine=create_engine('postgresql://lucas:cultura@localhost/espacios_culturales')

#realizaremos la importacion de cvs a trabajar mediante variable url1,url2,url3

url1='museo_10-05-2022.csv'     #poner la direccion del archivo museo.cvs
url2='cine_10-05-2022.csv'     #poner la direccion del archivo cine.cvs
url3='biblioteca_10-05-2022.csv'     #poner la direccion del archivo biblioteca.cvs

museo=pd.read_csv(url1)
cine=pd.read_csv(url2)
biblioteca=pd.read_csv(url3)

#realizaremos modificaciones al nombre de las columnas para que ambas tengan los mismos caracteres

museo=museo.rename({'categoria':'Categoría', 'provincia':'Provincia','localidad':'Localidad','nombre':'Nombre',
              'direccion':'Direccion','telefono':'Teléfono','fuente':'Fuente'}, axis=1)
cine=cine.rename({'Dirección':'Direccion'},axis=1)
biblio=biblioteca.rename({'Domicilio':'Direccion'}, axis=1)

#Finalizado el renombramiento de columnas pasaremos a la concatenacion pero primero se debe seleccionar las columnas que se exigen en el proyecto

museo2=museo[['Cod_Loc', 'IdProvincia', 'IdDepartamento', 'Categoría','Provincia', 'Localidad', 'Nombre',
       'Direccion', 'CP','Teléfono', 'Mail', 'Web']]
cine2=cine[['Cod_Loc', 'IdProvincia', 'IdDepartamento', 'Categoría','Provincia', 'Localidad', 'Nombre',
       'Direccion', 'CP','Teléfono', 'Mail', 'Web']]
biblio2=biblio[['Cod_Loc', 'IdProvincia', 'IdDepartamento', 'Categoría','Provincia', 'Localidad', 'Nombre',
       'Direccion', 'CP','Teléfono', 'Mail', 'Web']]

#Procederemos a la concatenacion de las tablas
espacios_culturales=pd.concat([museo2,cine2,biblio2])
espacios_culturales=espacios_culturales.set_index('Cod_Loc')
#realizamos este paso para que el index sea dicha columna mencionada por que
#por que a la hora de popular los datos , se incluira el index por defecto de python

#creamos la variable fecha para saber el dia que se insertaron los datos
ahora=datetime.now()
fecha=datetime.strftime(ahora,'%d/%m/%Y')
espacios_culturales['Fecha_insersion']=fecha

#Finalmente pasaremos a la insersion de datos hacia la tabla cultura que crearemos

#'IMPORTANTE LEER'se utiliza por primera vez cuando importamos nuestros datos
espacios_culturales.to_sql('cultura',con=engine,if_exists='replace')

#'IMPORTANTE LEER' se utilizara para nuevas actualizaciones  de datos
## espacios_culturales.to_sql('cultura',con=engine,if_exists='append')

#Realizaremos la creacion de la segunda tabla que llamaremos Registros

museo3=museo[['Categoría','Provincia','Fuente','IdProvincia']]
cine3=cine[['Categoría','Provincia','Fuente','IdProvincia']]
biblio3=biblio[['Categoría','Provincia','Fuente','IdProvincia']]


registros=pd.concat([museo3,cine3,biblio3])
registros=registros.groupby(['Provincia','Categoría','Fuente']).count()
registros=registros.rename({'IdProvincia':'Cantidad_registros'},axis=1)
registros['Fecha de insercion']=fecha
registros

#Finalizaremos con la populacion de los datos a la tabla registros que crearemos

#'IMPORTANTE LEER'se utiliza por primera vez cuando importamos nuestros datos
registros.to_sql('registros',con=engine,if_exists='replace')


#'IMPORTANTE LEER' se utilizara para nuevas actualizaciones  de datos
## espacios_culturales.to_sql('registros',con=engine,if_exists='append')

#Ahora crearemos la tercera tabla llamada info_cine donde se detallara la cantidad de butacas y provincias

informe_cine=cine.loc[:,['Provincia','Pantallas','Butacas','espacio_INCAA']]

info_cine=informe_cine.groupby(['Provincia']).sum()

info_cine['espacio_INCAA'] = informe_cine.groupby(['Provincia']).count()['espacio_INCAA']
info_cine['Fecha de insercion']=fecha
info_cine

#Populacion de datos a la tabla que crearemos llamado info_cine

#'IMPORTANTE LEER'se utiliza por primera vez cuando importamos nuestros datos
registros.to_sql('info_cine',con=engine,if_exists='replace')


#'IMPORTANTE LEER' se utilizara para nuevas actualizaciones  de datos
## espacios_culturales.to_sql('info_cine',con=engine,if_exists='append')