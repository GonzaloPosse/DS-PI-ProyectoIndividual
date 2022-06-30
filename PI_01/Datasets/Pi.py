from re import A
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
my_conn =create_engine('mysql+mysqldb://root:root123@localhost/rockingdata')
#Carga de los csv
clientes_2020 = pd.read_csv('Clientes_Dic2020.csv',sep=';')
clientes = pd.read_csv('Clientes.csv',sep=';')
localidad = pd.read_csv('Localidades.csv')
gasto = pd.read_csv('Gasto.csv')
compras = pd.read_csv('Compra.csv')
venta_dic = pd.read_csv('Venta_Dic2020.csv')
ventas = pd.read_csv('Venta.csv')
sucursales = pd.read_csv('Sucursales.csv')
proveedores = pd.read_csv('Proveedores.csv', encoding='latin1')
tipo_gasto = pd.read_csv('TiposDeGasto.csv')
canal_venta = pd.read_excel('CanalDeVenta.xlsx')

#Normalizacion de datos en todos los DataFrame
clientes_2020 = clientes_2020.sort_values(by='ID')
clientes = clientes.sort_values(by='ID')

clientes_fin = pd.concat([clientes, clientes_2020], axis=0)
clientes_fin = clientes_fin.drop(columns='col10')                       #Se elimina la columna "col10" que estaba vacia
clientes_fin = clientes_fin.fillna('N/D')                               #Se completan los datos faltandes con la secuencia "N/D" para no perder el registro


gasto['Fecha'] = pd.to_datetime(gasto['Fecha'])

compras['Fecha'] = pd.to_datetime(compras['Fecha'])
compras = compras.drop(['Fecha_Año','Fecha_Mes','Fecha_Periodo'], axis=1)
compras['Cantidad'] = compras['Cantidad'].astype('float')
compras['Precio'] = compras['Precio'].fillna(0)

venta_dic['Fecha'] = pd.to_datetime(venta_dic['Fecha'])
venta_dic['Fecha_Entrega'] = pd.to_datetime(venta_dic['Fecha_Entrega'])
ventas['Fecha'] = pd.to_datetime(ventas['Fecha'])
ventas['Fecha_Entrega'] = pd.to_datetime(ventas['Fecha_Entrega'])
ventas = pd.concat([ventas, venta_dic], axis=0)                         #Se juntan ambos DataFrames de ventas para tener uno solo

productos = ventas[(ventas['Cantidad'] == 1) & (ventas['Precio'] != np.nan)]    #Para tener una tabla con todos los productos
productos = productos.drop(['IdVenta','Fecha','Fecha_Entrega','IdCanal','IdCliente','IdSucursal','IdEmpleado'],axis=1)      #No me sirven estas tablas en la tabla producto
productos = productos.sort_values('IdProducto')
productos = productos.sort_values('Precio')
productos = productos.drop_duplicates(subset='IdProducto',keep='first')         #El keep first sirve para que tome el primer valor, y como anteriormente los ordenamos de menor a mayo, automaticamente se eliminan los otuliers



proveedores = proveedores.rename({'Address':'Direccion','City':'Ciudad','State':'Provincia','Country':'Pais',
                                'departamen':'Departamento'},axis=1)
proveedores['Direccion']=proveedores.Direccion.str.lower()
proveedores['Ciudad']=proveedores.Ciudad.str.lower()
proveedores['Provincia']=proveedores.Provincia.str.lower()
proveedores['Pais']=proveedores.Pais.str.lower()
proveedores['Departamento']=proveedores.Departamento.str.lower()

#Se crean varias tablas a partir de localidad para hacerla mas facil de trabajar a traves de ID
provincia = localidad
provincia = provincia.drop(['categoria','centroide_lat','centroide_lon','departamento_id','departamento_nombre',
                            'id','localidad_censal_id','localidad_censal_nombre','municipio_id','municipio_nombre'
                            ,'nombre'],axis=1)
provincia = provincia.drop_duplicates(subset='provincia_id',keep='first')
localidad = localidad.drop(['provincia_nombre'],axis=1)
departamentos = localidad.drop(['categoria','centroide_lat','centroide_lon','provincia_nombre',
                            'id','localidad_censal_id','localidad_censal_nombre','municipio_id','municipio_nombre'
                            ,'nombre'],axis=1)
departamentos = departamentos.drop_duplicates(subset='departamento_id', keep='first')
localidad = localidad.drop(['departamento_nombre'],axis=1)
municipios = localidad.drop(['categoria','centroide_lat','centroide_lon',
                            'id','localidad_censal_id','localidad_censal_nombre','provincia_id','departamento_id'
                            ,'nombre'],axis=1)
municipios = municipios.drop_duplicates(subset='municipio_id',keep='first')
localidad = localidad.drop(['municipio_nombre'],axis=1)

#Subida de datos a la base de datos
localidad.to_sql(con=my_conn,name='localidad',if_exists='append',index=False)
ventas.to_sql(con=my_conn,name='Ventas',if_exists='append',index=False)
clientes_fin.to_sql(con=my_conn,name='Clientes',if_exists='append',index=False)
gasto.to_sql(con=my_conn,name='Gasto',if_exists='append',index=False)
ventas.to_sql(con=my_conn,name='Ventas',if_exists='append',index=False)
sucursales.to_sql(con=my_conn,name='Sucursales',if_exists='replace',index=False)
proveedores.to_sql(con=my_conn,name='Proveedores',if_exists='replace',index=False)
tipo_gasto.to_sql(con=my_conn,name='TipoDeGasto',if_exists='replace',index=False)
canal_venta.to_sql(con=my_conn,name='Canal De Venta',if_exists='replace',index=False)
productos.to_sql(con=my_conn,name='Productos',if_exists='replace',index=False)
provincia.to_sql(con=my_conn,name='Provincias',if_exists='replace',index=False)
municipios.to_sql(con=my_conn,name='Municipios',if_exists='replace',index=False)
departamentos.to_sql(con=my_conn,name='Departamentos',if_exists='replace',index=False)
