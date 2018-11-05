import pygdf as gd

import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel
from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status

from blazingdb.protocol.interpreter import InterpreterMessage
from blazingdb.protocol.orchestrator import OrchestratorMessageType
from blazingdb.protocol.gdf import gdf_columnSchema

from libgdf_cffi import ffi
from pygdf.datetime import DatetimeColumn
from pygdf.numerical import NumericalColumn

import pyarrow as pa
from pygdf import _gdf
from pygdf import column
from pygdf import numerical
from pygdf import DataFrame
from pygdf.dataframe import Series
from pygdf.buffer import Buffer
from pygdf import utils

from numba import cuda
import numpy as np
import pandas as pd

# NDarray device helper
from numba import cuda
from numba.cuda.cudadrv import driver, devices
require_context = devices.require_context
current_context = devices.get_context
gpus = devices.gpus

class ResultSetHandle:
    columns = None
    token = None
    interpreter_path = None
    handle = None
    client = None
    def __init__(self,columns, token, interpreter_path, handle, client):
        self.columns = columns
        self.token = token
        self.interpreter_path = interpreter_path
        self.handle = handle
        self.client = client

    def __del__(self):
        del self.handle
        self.client.free_result(self.token,self.interpreter_path)



def run_query(sql, tables):
    """
    Run a SQL query over a dictionary of GPU DataFrames.
    Parameters
    ----------
    sql : str
        The SQL query.
    tables : dict[str]:GPU ``DataFrame``
        A dictionary where each key is the table name and each value is the
        associated GPU ``DataFrame`` object.
    Returns
    -------
    A GPU ``DataFrame`` object that contains the SQL query result.
    Examples
    --------
    >>> import pygdf as gd
    >>> import pyblazing
    >>> products = gd.DataFrame({'month': [2, 8, 11], 'sales': [12.1, 20.6, 13.79]})
    >>> cats = gd.DataFrame({'age': [12, 28, 19], 'weight': [5.3, 9, 7.68]})
    >>> tables = {'products': products, 'cats': cats}
    >>> result = pyblazing.run_query('select * from products, cats limit 2', tables)
    >>> type(result)
    pygdf.dataframe.DataFrame
    """
    return _private_run_query(sql, tables)


def run_query_pandas(sql, tables):
    """
    Run a SQL query over a dictionary of Pandas DataFrames.
    This convenience function will convert each table from Pandas DataFrame
    to GPU ``DataFrame`` and then will use ``run_query``.
    Parameters
    ----------
    sql : str
        The SQL query.
    tables : dict[str]:Pandas DataFrame
        A dictionary where each key is the table name and each value is the
        associated Pandas DataFrame object.
    Returns
    -------
    A GPU ``DataFrame`` object that contains the SQL query result.
    Examples
    --------
    >>> import pandas as pd
    >>> import pyblazing
    >>> products = pd.DataFrame({'month': [2, 8, 11], 'sales': [12.1, 20.6, 13.79]})
    >>> cats = pd.DataFrame({'age': [12, 28, 19], 'weight': [5.3, 9, 7.68]})
    >>> tables = {'products': products, 'cats': cats}
    >>> result = pyblazing.run_query_pandas('select * from products, cats limit 2', tables)
    >>> type(result)
    pygdf.dataframe.DataFrame
    """

    gdf_tables = {}
    for table, df in tables.items():
        gdf = gd.DataFrame.from_pandas(df)
        gdf_tables[table] = gdf

    return run_query(sql, gdf_tables)


# TODO complete API docs
# WARNING EXPERIMENTAL
def _run_query_arrow(sql, tables):
    """
    Run a SQL query over a dictionary of pyarrow.Table.
    This convenience function will convert each table from pyarrow.Table
    to Pandas DataFrame and then will use ``run_query_pandas``.  
    Parameters
    ----------
    sql : str
        The SQL query.
    tables : dict[str]:pyarrow.Table
        A dictionary where each key is the table name and each value is the 
        associated pyarrow.Table object.
    Returns
    -------
    A GPU ``DataFrame`` object that contains the SQL query result.
    Examples
    --------
    >>> import pyarrow as pa
    >>> import pyblazing
    >>> products = pa.RecordBatchStreamReader('products.arrow').read_all()
    >>> cats = pa.RecordBatchStreamReader('cats.arrow').read_all()
    >>> tables = {'products': products, 'cats': cats}
    >>> result = pyblazing.run_query_arrow('select * from products, cats limit 2', tables)
    >>> type(result)
    pygdf.dataframe.DataFrame
    """

    pandas_tables = {}
    for table, arr in tables.items():
        df = arr.to_pandas()
        pandas_tables[table] = df

    return run_query_pandas(sql, pandas_tables)


def _lots_of_stuff():
    pass


class PyConnector:
    def __init__(self, orchestrator_path):
        self._orchestrator_path = orchestrator_path

    def __del__(self):
        self.close_connection()

    def connect(self):
        # TODO find a way to print only for debug mode (add verbose arg)
        #print("open connection")
        authSchema = blazingdb.protocol.orchestrator.AuthRequestSchema()

        requestBuffer = blazingdb.protocol.transport.channel.MakeAuthRequestBuffer(
            OrchestratorMessageType.AuthOpen, authSchema)

        responseBuffer = self._send_request(
            self._orchestrator_path, requestBuffer)

        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            print(errorResponse.errors)
            raise Error(errorResponse.errors)
        responsePayload = blazingdb.protocol.orchestrator.AuthResponseSchema.From(
            response.payload)

        # TODO find a way to print only for debug mode (add verbose arg)
        # print(responsePayload.accessToken)
        self.accessToken = responsePayload.accessToken

    def _send_request(self, unix_path, requestBuffer):
        connection = blazingdb.protocol.UnixSocketConnection(unix_path)
        client = blazingdb.protocol.Client(connection)
        return client.send(requestBuffer)

    def run_dml_query_token(self, query, tableGroup):
        dmlRequestSchema = blazingdb.protocol.orchestrator.BuildDMLRequestSchema(query, tableGroup)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DML, self.accessToken, dmlRequestSchema)
        responseBuffer = self._send_request(
            self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            raise Error(errorResponse.errors)
        dmlResponseDTO = blazingdb.protocol.orchestrator.DMLResponseSchema.From(
            response.payload)
        return dmlResponseDTO.resultToken, dmlResponseDTO.nodeConnection.path

    def run_dml_query(self, query, tableGroup):
        # TODO find a way to print only for debug mode (add verbose arg)
        # print(query)
        dmlRequestSchema = blazingdb.protocol.orchestrator.BuildDMLRequestSchema(
            query, tableGroup)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DML,
                                                                               self.accessToken, dmlRequestSchema)
        responseBuffer = self._send_request(
            self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            raise Error(errorResponse.errors)
        dmlResponseDTO = blazingdb.protocol.orchestrator.DMLResponseSchema.From(
            response.payload)
        return self._get_result(dmlResponseDTO.resultToken, dmlResponseDTO.nodeConnection.path)

    def run_ddl_create_table(self, tableName, columnNames, columnTypes, dbName):
        # TODO find a way to print only for debug mode (add verbose arg)
        #print('create table: ' + tableName)
        # print(columnNames)
        # print(columnTypes)
        # print(dbName)
        dmlRequestSchema = blazingdb.protocol.orchestrator.DDLCreateTableRequestSchema(name=tableName,
                                                                                       columnNames=columnNames,
                                                                                       columnTypes=columnTypes,
                                                                                       dbName=dbName)

        # TODO find a way to print only for debug mode (add verbose arg)
        # print(dmlRequestSchema)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DDL_CREATE_TABLE,
                                                                               self.accessToken, dmlRequestSchema)

        responseBuffer = self._send_request(
            self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            raise Error(errorResponse.errors)

        # TODO find a way to print only for debug mode (add verbose arg)
        # print(response.status)

        return response.status

    def run_ddl_drop_table(self, tableName, dbName):
        # TODO find a way to print only for debug mode (add verbose arg)
        #print('drop table: ' + tableName)

        dmlRequestSchema = blazingdb.protocol.orchestrator.DDLDropTableRequestSchema(
            name=tableName, dbName=dbName)
        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(OrchestratorMessageType.DDL_DROP_TABLE,
                                                                               self.accessToken, dmlRequestSchema)
        responseBuffer = self._send_request(
            self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            raise Error(errorResponse.errors)

        # TODO find a way to print only for debug mode (add verbose arg)
        # print(response.status)

        return response.status

    def close_connection(self):
        # TODO find a way to print only for debug mode (add verbose arg)
        #print("close connection")

        authSchema = blazingdb.protocol.orchestrator.AuthRequestSchema()

        requestBuffer = blazingdb.protocol.transport.channel.MakeAuthRequestBuffer(
            OrchestratorMessageType.AuthClose, authSchema)

        responseBuffer = self._send_request(
            self._orchestrator_path, requestBuffer)
        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)
        if response.status == Status.Error:
            errorResponse = blazingdb.protocol.transport.channel.ResponseErrorSchema.From(
                response.payload)
            print(errorResponse.errors)

        # TODO find a way to print only for debug mode (add verbose arg)
        # print(response.status)

    def free_result(self, result_token, interpreter_path):
        getResultRequest = blazingdb.protocol.interpreter.GetResultRequestSchema(
            resultToken=result_token)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
            InterpreterMessage.FreeResult, self.accessToken, getResultRequest)

        responseBuffer = self._send_request(
            interpreter_path, requestBuffer)

        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)

        if response.status == Status.Error:
            raise ValueError('Error status')

        # TODO find a way to print only for debug mode (add verbose arg)
        #print('free result OK!')

    def _get_result(self, result_token, interpreter_path):

        getResultRequest = blazingdb.protocol.interpreter.GetResultRequestSchema(
            resultToken=result_token)

        requestBuffer = blazingdb.protocol.transport.channel.MakeRequestBuffer(
            InterpreterMessage.GetResult, self.accessToken, getResultRequest)

        responseBuffer = self._send_request(
            interpreter_path, requestBuffer)

        response = blazingdb.protocol.transport.channel.ResponseSchema.From(
            responseBuffer)

        if response.status == Status.Error:
            raise ValueError('Error status')

        queryResult = blazingdb.protocol.interpreter.GetQueryResultFrom(
            response.payload)
        return queryResult


def gen_data_frame(nelem, name, dtype):
    pdf = pd.DataFrame()
    pdf[name] = np.arange(nelem, dtype=dtype)
    df = DataFrame.from_pandas(pdf)
    return df


def get_ipc_handle_for(df):
    cffiView = df._column.cffi_view
    ipch = df._column._data.mem.get_ipc_handle()
    return bytes(ipch._ipc_handle.handle)


def gdf_column_type_to_str(dtype):
    str_dtype = {
        0: 'GDF_invalid',
        1: 'GDF_INT8',
        2: 'GDF_INT16',
        3: 'GDF_INT32',
        4: 'GDF_INT64',
        5: 'GDF_FLOAT32',
        6: 'GDF_FLOAT64',
        7: 'GDF_DATE32',
        8: 'GDF_DATE64',
        9: 'GDF_TIMESTAMP',
        10: 'GDF_CATEGORY',
        11: 'GDF_STRING',
        12: 'GDF_UINT8',
        13: 'GDF_UINT16',
        14: 'GDF_UINT32',
        15: 'GDF_UINT64',
        16: 'N_GDF_TYPES'
    }

    return str_dtype[dtype]


def _get_table_def_from_gdf(gdf):
    cols = gdf.columns.values.tolist()

    # TODO find a way to print only for debug mode (add verbose arg)
    # print(cols)

    types = []
    for key, column in gdf._cols.items():
        dtype = column._column.cffi_view.dtype
        types.append(gdf_column_type_to_str(dtype))
    return cols, types


def _reset_table(client, table, gdf):
    client.run_ddl_drop_table(table, 'main')
    cols, types = _get_table_def_from_gdf(gdf)
    client.run_ddl_create_table(table, cols, types, 'main')

# TODO add valid support


def _to_table_group(tables):
    database_name = 'main'
    tableGroup = {'name': database_name}
    blazing_tables = []
    for table, gdf in tables.items():
        # TODO columnNames should have the columns of the query (check this)
        blazing_table = {'name': database_name + '.' + table,
                         'columnNames': gdf.columns.values.tolist()}
        blazing_columns = []

        for column in gdf.columns:
            dataframe_column = gdf._cols[column]
            # TODO support more column types
            numerical_column = dataframe_column._column
            data_sz = numerical_column.cffi_view.size
            dtype = numerical_column.cffi_view.dtype

            data_ipch = get_ipc_handle_for(dataframe_column)

            # TODO this valid data is fixed and is invalid
	    #felipe doesnt undertand why we need this we can send null
	    #if the bitmask is not valid
            #sample_valid_df = gen_data_frame(data_sz, 'valid', np.int8)
            #valid_ipch = get_ipc_handle_for(sample_valid_df['valid'])

            blazing_column = {
                'data': data_ipch,
                'valid': None,  # TODO we should use valid mask
                'size': data_sz,
                'dtype': dataframe_column._column.cffi_view.dtype,
                'null_count': 0,
                'dtype_info': 0
            }
            blazing_columns.append(blazing_column)

        blazing_table['columns'] = blazing_columns
        blazing_tables.append(blazing_table)

    tableGroup['tables'] = blazing_tables
    return tableGroup


def _get_client_internal():
    client = PyConnector('/tmp/orchestrator.socket')

    try:
        client.connect()
    except Error as err:
        print(err)

    return client



__blazing__global_client = _get_client_internal()

def _get_client():
    return __blazing__global_client

def _private_run_query(sql, tables):
    client = _get_client()

    try:
        for table, gdf in tables.items():
            _reset_table(client, table, gdf)
    except Error as err:
        print(err)
    ipchandles = []
    resultSet = None
    token = None
    interpreter_path = None
    try:
        tableGroup = _to_table_group(tables)
        token, interpreter_path = client.run_dml_query_token(sql, tableGroup)
        print(token)
        print(interpreter_path)
        resultSet = client._get_result(token, interpreter_path)

        def cffi_view_to_column_mem(cffi_view):
            data = _gdf._as_numba_devarray(intaddr=int(ffi.cast("uintptr_t",
                                                                cffi_view.data)),
                                           nelem=cffi_view.size,
                                           dtype=_gdf.gdf_to_np_dtype(cffi_view.dtype))
            mask = None
            return data, mask

        def from_cffi_view(cffi_view):
            data_mem, mask_mem = cffi_view_to_column_mem(cffi_view)
            data_buf = Buffer(data_mem)
            mask = None
            return column.Column(data=data_buf, mask=mask)

        def _open_ipc_array(handle, shape, dtype, strides=None, offset=0):
            dtype = np.dtype(dtype)
            # compute size
            size = np.prod(shape) * dtype.itemsize
            # manually recreate the IPC mem handle
            handle = driver.drvapi.cu_ipc_mem_handle(*handle)
            # use *IpcHandle* to open the IPC memory
            ipchandle = driver.IpcHandle(None, handle, size, offset=offset)
            return ipchandle, ipchandle.open_array(current_context(), shape=shape,
                                                   strides=strides, dtype=dtype)

        gdf_columns = []
        
        for i, c in enumerate(resultSet.columns):
            assert len(c.data) == 64
            ipch, data_ptr = _open_ipc_array(
                c.data, shape=c.size, dtype=_gdf.gdf_to_np_dtype(c.dtype))
            ipchandles.append(ipch)
            #gdf_col = _gdf.columnview_from_devary(data_ptr, ffi.NULL)
            #newcol = from_cffi_view(gdf_col).copy()
            gdf_col = _gdf.columnview_from_devary(data_ptr, ffi.NULL)
            gdf_columns.append(from_cffi_view(gdf_col))

        df = DataFrame()
        for k, v in zip(resultSet.columnNames, gdf_columns):
            df[str(k)] = v

        resultSet.columns = df

        # @todo close ipch, see one solution at ()
        # print(df)
        # for ipch in ipchandles:
        #     ipch.close()

    except Error as err:
        print(err)

    return_result = ResultSetHandle(resultSet.columns, token, interpreter_path, ipchandles, client)
    return return_result
