#include <Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "deviceapps.pb-c.h"

#define MAGIC  0xFFFFFFFF
#define DEVICE_APPS_TYPE 1

typedef struct pbheader_s {
    uint32_t magic;
    uint16_t type;
    uint16_t length;
} pbheader_t;
#define PBHEADER_INIT {MAGIC, 0, 0}


// Read iterator of Python dicts
// Pack them to DeviceApps protobuf and write to file with appropriate header
// Return number of written bytes as Python integer
static PyObject* py_deviceapps_xwrite_pb(PyObject* self, PyObject* args) {
    const char* path;
    PyObject* o;

    const char* device_type;
    const char* device_id;

    double lat;
    double lon;

    long total_size;
    total_size = 0;
    int n_apps;

    if (!PyArg_ParseTuple(args, "Os", &o, &path))
        return NULL;

    pbheader_t header = PBHEADER_INIT;

    int i;
    int l;
    int j;

    l = PyList_Size(o);

    FILE *f;
    f = fopen(path, "wb");

    if(!f){
    	PyErr_Format(PyExc_OSError, "No such file: %s", path);
        return NULL;
    }


    for (i=0; i<l; i++) {
    	PyObject* py_device_info = PyList_GetItem(o, i);
    	
    	PyObject* py_device = PyDict_GetItemString(py_device_info, "device");

    
    	PyObject* py_apps = PyDict_GetItemString(py_device_info, "apps");
    	n_apps = PyList_Size(py_apps);

    	uint32_t apps[n_apps];
    	uint32_t app;


    	for (j=0; j<n_apps; j++){
    		PyObject* py_app = PyList_GetItem(py_apps, j);
    		app = PyInt_AsLong(py_app);
    		apps[j] = app;
    	}

		DeviceApps msg = DEVICE_APPS__INIT;
	    DeviceApps__Device device = DEVICE_APPS__DEVICE__INIT;
	    void *buf;
	    unsigned len;


	    PyStringObject* py_device_type = PyDict_GetItemString(py_device, "type");
	    if (py_device_type){
	    	device_type = PyString_AsString(py_device_type);
	    	device.has_type = 1;
		    device.type.data = (uint8_t*)device_type;
		    device.type.len = strlen(device_type);
		}
		else
			device.has_type = 0;
		

    	PyStringObject* py_device_id = PyDict_GetItemString(py_device, "id");
    	if (py_device_type){	
	    	device_id = PyString_AsString(py_device_id);
		    device.has_id = 1;
		    device.id.data = (uint8_t*)device_id;
		    device.id.len = strlen(device_id);
		}
		else
			device.has_id = 0;

	    
	    msg.device = &device;


	    PyObject* py_lat = PyDict_GetItemString(py_device_info, "lat");
	    if (py_lat){	
	    	lat = PyFloat_AsDouble(py_lat);
	    	msg.has_lat = 1;
	    	msg.lat = lat;
	    }
	    else
	    	msg.has_lat = 0;

	    PyObject* py_lon = PyDict_GetItemString(py_device_info, "lon");
    	
	    if (py_lon){	
	    	lon = PyFloat_AsDouble(py_lon);
	    	msg.has_lon = 1;
	    	msg.lon = lon;
	    }
	    else
	    	msg.has_lon = 0;

	    msg.n_apps = n_apps;
	    msg.apps = malloc(sizeof(uint32_t) * msg.n_apps);
	    for(j = 0; j < n_apps; j++)
	    	msg.apps[j] = apps[j];
	    len = device_apps__get_packed_size(&msg);

	    buf = malloc(len);
	    device_apps__pack(&msg, buf);

	    header.type = DEVICE_APPS_TYPE;
	    header.length = len;
	    fwrite(&header, sizeof(pbheader_t), 1, f);
	    total_size += sizeof(pbheader_t);

	    fwrite(buf, len, 1, f);
	    total_size += len;

	    free(msg.apps);
	    free(buf);

    }

    fclose(f);
   

    PyObject * result = Py_BuildValue("l",total_size);

    return result;
}

// ****************
// *UNPACK TO LIST*
// ****************


// Unpack only messages with type == DEVICE_APPS_TYPE
// Return list of Python dicts
static PyObject* py_deviceapps_xread_list_pb(PyObject* self, PyObject* args) {
    DeviceApps *msg;
    DeviceApps__Device *device = DEVICE_APPS__DEVICE__INIT;

    const char* path;
    int n_apps;

    if (!PyArg_ParseTuple(args, "s", &path))
        return NULL;

    PyObject *py_deviceapps;
    PyObject *py_deviceapp;
    PyObject *py_device;
    PyObject *py_apps;
    PyObject *py_app;
    PyObject *py_device_id;
    PyObject *py_device_type;
    PyObject *py_lat;
    PyObject *py_lon;

    py_deviceapps = PyList_New(0);

    FILE *f;
    f = fopen(path, "rb");
    if(!f){
    	PyErr_Format(PyExc_OSError, "No such file: %s", path);
        return NULL;
    }
	void* buf;

    pbheader_t header = PBHEADER_INIT;
    int cb;

    while (1) {
	    fread(&header, sizeof(pbheader_t), 1, f);
	    if (feof(f))
	    	break;
	    buf = malloc(header.length);
	    fread(buf, header.length, 1, f);
    	if (header.type == DEVICE_APPS_TYPE){
	    	msg = device_apps__unpack(NULL, header.length, buf);
	    	
	    	device = msg->device;
	    	n_apps = msg->n_apps;

	    	int j;
	    	py_apps = PyList_New(0);
	    	for(j = 0; j < n_apps; j++){
	    		py_app = PyInt_FromLong(msg->apps[j]);
		    	PyList_Append(py_apps, py_app);
		    	Py_DECREF(py_app);
	    	}

		    py_device = PyDict_New();
		    if (device->has_id==1){
				py_device_id = PyString_FromStringAndSize(device->id.data, device->id.len);
				PyDict_SetItemString(py_device, "id", py_device_id);
				Py_DECREF(py_device_id);
			}

			if (device->has_type==1){
				py_device_type = PyString_FromStringAndSize(device->type.data, device->type.len);
				PyDict_SetItemString(py_device, "type", py_device_type);
				Py_DECREF(py_device_type);
			}

			py_deviceapp = PyDict_New();
			PyDict_SetItemString(py_deviceapp, "device", py_device);
			Py_DECREF(py_device);

		    if (msg->has_lat==1){
	    		py_lat = PyFloat_FromDouble(msg->lat);	
	    		PyDict_SetItemString(py_deviceapp, "lat", py_lat);
				Py_DECREF(py_lat);
			}

	    	if (msg->has_lon==1){
	    		py_lon = PyFloat_FromDouble(msg->lon);
	    		PyDict_SetItemString(py_deviceapp, "lon", py_lon);
				Py_DECREF(py_lon);
			}
			PyDict_SetItemString(py_deviceapp, "apps", py_apps);
			Py_DECREF(py_apps);

		    PyList_Append(py_deviceapps, py_deviceapp);
		    Py_DECREF(py_deviceapp);

		    device_apps__free_unpacked(msg, NULL);
	    	
    	}
    	free(buf);    	

	}

	fclose(f);
    return py_deviceapps;
}



// ********************
// *UNPACK TO ITERATOR*
// ********************


typedef struct {
    PyObject_HEAD
    FILE *f;
} PBGen;


PyObject* PBGen_dealloc(PBGen* pbgen) {
    fclose(pbgen->f);
}


static PyObject *
PBGen_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    PyObject *py_file;

    if (!PyArg_UnpackTuple(args, "pbgen", 1, 1, &py_file))
        return NULL;
    if (!PyString_Check(py_file)) {
        PyErr_SetString(PyExc_TypeError, "pbgen() expects a file path");
        return NULL;
    }

    PBGen *pbgen = (PBGen *)type->tp_alloc(type, 0);
    if (!pbgen)
        return NULL;

    pbgen->f = fopen(PyString_AsString(py_file), "rb");
    if(!pbgen->f){
    	PyErr_Format(PyExc_OSError, "No such file: %s", PyString_AsString(py_file));
        return NULL;
    }

    return (PyObject *)pbgen;
}


PyObject* PBGen_next(PBGen* pbgen) {
    DeviceApps *msg;
    DeviceApps__Device *device = DEVICE_APPS__DEVICE__INIT;

    int n_apps;

    PyObject *py_deviceapp;
    PyObject *py_device;
    PyObject *py_apps;
    PyObject *py_app;
    PyObject *py_device_id;
    PyObject *py_device_type;
    PyObject *py_lat;
    PyObject *py_lon;

	void* buf;
    pbheader_t header = PBHEADER_INIT;

    while (1) {
	    fread(&header, sizeof(pbheader_t), 1, pbgen->f);
	    if (feof(pbgen->f))
	    	return NULL;
	    if (header.type == DEVICE_APPS_TYPE){
	    	break;
    	}
    }

    buf = malloc(header.length);
    fread(buf, header.length, 1, pbgen->f);

	msg = device_apps__unpack(NULL, header.length, buf);
	
	device = msg->device;
	n_apps = msg->n_apps;

	int j;
	py_apps = PyList_New(0);
	for(j = 0; j < n_apps; j++){
		py_app = PyInt_FromLong(msg->apps[j]);
    	PyList_Append(py_apps, py_app);
    	Py_DECREF(py_app);
	}

    py_device = PyDict_New();
    if (device->has_id==1){
		py_device_id = PyString_FromStringAndSize(device->id.data, device->id.len);
		PyDict_SetItemString(py_device, "id", py_device_id);
		Py_DECREF(py_device_id);
	}

	if (device->has_type==1){
		py_device_type = PyString_FromStringAndSize(device->type.data, device->type.len);
		PyDict_SetItemString(py_device, "type", py_device_type);
		Py_DECREF(py_device_type);
	}


	py_deviceapp = PyDict_New();
	PyDict_SetItemString(py_deviceapp, "device", py_device);
	Py_DECREF(py_device);

    if (msg->has_lat==1){
		py_lat = PyFloat_FromDouble(msg->lat);	
		PyDict_SetItemString(py_deviceapp, "lat", py_lat);
		Py_DECREF(py_lat);
	}

	if (msg->has_lon==1){
		py_lon = PyFloat_FromDouble(msg->lon);
		PyDict_SetItemString(py_deviceapp, "lon", py_lon);
		Py_DECREF(py_lon);
	}
	PyDict_SetItemString(py_deviceapp, "apps", py_apps);
	Py_DECREF(py_apps);

    device_apps__free_unpacked(msg, NULL);
	
	free(buf);
	return py_deviceapp;    
};


PyTypeObject PBGen_Type = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "pbgen",                       /* tp_name */
    sizeof(PBGen),            /* tp_basicsize */
    0,                              /* tp_itemsize */
    (destructor) PBGen_dealloc,     /* tp_dealloc */
    0,                              /* tp_print */
    0,                              /* tp_getattr */
    0,                              /* tp_setattr */
    0,                              /* tp_reserved */
    0,                              /* tp_repr */
    0,                              /* tp_as_number */
    0,                              /* tp_as_sequence */
    0,                              /* tp_as_mapping */
    0,                              /* tp_hash */
    0,                              /* tp_call */
    0,                              /* tp_str */
    0,                              /* tp_getattro */
    0,                              /* tp_setattro */
    0,                              /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,             /* tp_flags */
    0,                              /* tp_doc */
    0,                              /* tp_traverse */
    0,                              /* tp_clear */
    0,                              /* tp_richcompare */
    0,                              /* tp_weaklistoffset */
    PyObject_SelfIter,              /* tp_iter */
    (iternextfunc)PBGen_next,       /* tp_iternext */
    0,                              /* tp_methods */
    0,                              /* tp_members */
    0,                              /* tp_getset */
    0,                              /* tp_base */
    0,                              /* tp_dict */
    0,                              /* tp_descr_get */
    0,                              /* tp_descr_set */
    0,                              /* tp_dictoffset */
    0,                              /* tp_init */
    PyType_GenericAlloc,            /* tp_alloc */
    PBGen_new,                      /* tp_new */
};




// Unpack only messages with type == DEVICE_APPS_TYPE
// Return iterator of Python dicts
static PyObject* py_deviceapps_xread_pb(PyObject* self, PyObject* args) {
    return PyObject_CallObject((PyObject *) &PBGen_Type, args);
};


static PyMethodDef PBMethods[] = {
     {"deviceapps_xwrite_pb", py_deviceapps_xwrite_pb, METH_VARARGS, "Write serialized protobuf to file fro iterator"},
     {"deviceapps_xread_list_pb", py_deviceapps_xread_list_pb, METH_VARARGS, "Deserialize protobuf from file, return list"},
     {"deviceapps_xread_pb", py_deviceapps_xread_pb, METH_VARARGS, "Deserialize protobuf from file, return iterator"},
     {NULL, NULL, 0, NULL}
};


PyMODINIT_FUNC initpb(void) {
	(void) Py_InitModule("pb", PBMethods);
}

