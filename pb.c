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

    printf("Write to: %s\n", path);

    pbheader_t header = PBHEADER_INIT;

    int i;
    int l;
    
    int j;

    l = PyList_Size(o);

    printf("Got %d devices\n", l);

    FILE *f;
    f = fopen(path, "wb");

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

// Unpack only messages with type == DEVICE_APPS_TYPE
// Return iterator of Python dicts
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



static PyMethodDef PBMethods[] = {
     {"deviceapps_xwrite_pb", py_deviceapps_xwrite_pb, METH_VARARGS, "Write serialized protobuf to file fro iterator"},
     {"deviceapps_xread_list_pb", py_deviceapps_xread_list_pb, METH_VARARGS, "Deserialize protobuf from file, return list"},
     {NULL, NULL, 0, NULL}
};


PyMODINIT_FUNC initpb(void) {
     (void) Py_InitModule("pb", PBMethods);
}
