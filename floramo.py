import ssl
import sys
import re
import json
import os.path
import argparse
import numpy as np
import paho.mqtt.client as mqtt
from unidecode import unidecode
from time import time, sleep, localtime, strftime
from datetime import datetime
import pytz

# Scikit-Learn
# import skfuzzy as fuzz
# from skfuzzy import control as ctrl
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.model_selection import train_test_split

# Firebase Admin (SDK)
import firebase_admin
from firebase_admin import credentials, db

# Configuration parser
from configparser import ConfigParser

# Notifier (src: https://github.com/bb4242/sdnotify)
import sdnotify 

# CLI text coloring
from colorama import init as colorama_init
from colorama import Fore, Back, Style

# Mi Flora sensor parameters
from miflora.miflora_poller import MiFloraPoller, MI_LIGHT, MI_TEMPERATURE, MI_CONDUCTIVITY, MI_BATTERY, MI_MOISTURE

# Bluetooth
from btlewrap import BluepyBackend, GatttoolBackend, BluetoothBackendException
from bluepy.btle import BTLEException

# Signal (for MQTT / WIFI / Bluetooth)
from signal import signal, SIGPIPE, SIG_DFL

from collections import OrderedDict
from time import time, sleep, localtime, strftime

signal(SIGPIPE,SIG_DFL)
project_name = 'MiFLora-Client'


# Firebase Admin JSON
databaseURL = 'https://moniflora-7d3a3-default-rtdb.firebaseio.com/'

cred = credentials.Certificate('/home/andresaftari/moniflora-7d3a3-firebase-adminsdk-xtibx-0cba2b83ee.json')
firebase_admin.initialize_app(cred, {
    'databaseURL': databaseURL,
})

ref = db.reference("/")


# Variable untuk Fuzzy
# temperature = ctrl.Antecedent(np.arange(0, 41, 1), 'temperature')
# light_intensity = ctrl.Antecedent(np.arange(0, 101, 1), 'light_intensity')
# conductivity = ctrl.Antecedent(np.arange(0, 201, 1), 'conductivity')
# condition = ctrl.Consequent(np.arange(0, 101, 1), 'condition')


# Membership function untuk Fuzzy
# temperature['low'] = fuzz.trimf(temperature.universe, [0, 0, 21])
# temperature['optimum'] = fuzz.trimf(temperature.universe, [21, 23.5, 26])
# temperature['high'] = fuzz.trimf(temperature.universe, [26, 40, 40])

# light_intensity['low'] = fuzz.trimf(light_intensity.universe, [0, 0, 5000])
# light_intensity['optimum'] = fuzz.trimf(light_intensity.universe, [10000, 15000, 20000])
# light_intensity['high'] = fuzz.trimf(light_intensity.universe, [20000, 100000, 100000])

# conductivity['low'] = fuzz.trimf(conductivity.universe, [0, 0, 1])
# conductivity['optimum'] = fuzz.trimf(conductivity.universe, [1, 2, 3])
# conductivity['high'] = fuzz.trimf(conductivity.universe, [3, 200, 200])

# condition['optimum'] = fuzz.trimf(condition.universe, [0, 30, 60])
# condition['caution'] = fuzz.trimf(condition.universe, [40, 50, 80])
# condition['extreme'] = fuzz.trimf(condition.universe, [70, 100, 100])


# Rules untuk Fuzzy
# rule1 = ctrl.Rule(temperature['optimum'] & light_intensity['optimum'] & conductivity['optimum'], condition['optimum'])
# rule2 = ctrl.Rule((temperature['low'] | temperature['high']) & (light_intensity['low'] | light_intensity['high']) & (conductivity['low'] | conductivity['high']), condition['extreme'])
# rule3 = ctrl.Rule(temperature['optimum'] | light_intensity['optimum'] | conductivity['optimum'], condition['caution'])


# Control system untuk Fuzzy
# system = ctrl.ControlSystem([rule1, rule2, rule3])
# output = ctrl.ControlSystemSimulation(system)


# List data untuk nampung banyak data
list_data = []

# Firebase Reference
fb_sensor = ref.child('sensor')


# Collect features and labels
features = []
labels = []


# Sensor parameter
parameters = OrderedDict([
    (MI_LIGHT, dict(name="LightIntensity", name_pretty='Sunlight Intensity', typeformat='%d', unit='lx', device_class="illuminance", state_class="measurement")),
    (MI_TEMPERATURE, dict(name="AirTemperature", name_pretty='Air Temperature', typeformat='%.1f', unit='°C', device_class="temperature", state_class="measurement")),
    (MI_CONDUCTIVITY, dict(name="SoilConductivity", name_pretty='Soil Conductivity/Fertility', typeformat='%d', unit='µS/cm', state_class="measurement")),
    (MI_MOISTURE, dict(name='SoilMoisture', name_pretty='Soil Moisture', typeformat='%d', unit='%', device_class="humidity", state_class="measurement")),
    (MI_BATTERY, dict(name="Battery", name_pretty='Sensor Battery Level', typeformat='%d', unit='%', device_class="battery", state_class="measurement"))
])


# Harus menggunakan Python3
if False:
    print('Program ini hanya dapat berjalan dengan Python3.x runtime / environment.', file=sys.stderr)


# Argument parsing
parser = argparse.ArgumentParser(description=project_name)
parser.add_argument('--config_dir', help='SET directory sama seperti lokasi "config.ini"', default=sys.path[0])
parse_args = parser.parse_args()


# Intro program
colorama_init()
print(Fore.CYAN + Style.BRIGHT)
print(project_name)
print('===== Set config.ini untuk menyesuaikan environment sensor =====')
print(Style.RESET_ALL)

# System Service Notifier (src: https://github.com/bb4242/sdnotify)
sd_notifier = sdnotify.SystemdNotifier()


# Logging function
def print_out(text, error = False, warning=False, sd_notify=False, console=True):
    timestamp = strftime('%Y-%m-%d %H:%M:%S', localtime())
    
    if console:
        # Format untuk print error
        if error:
            print(Fore.RED + Style.BRIGHT + '[{}] '.format(timestamp) + Style.RESET_ALL + '{}'.format(text) + Style.RESET_ALL, file=sys.stderr)
        # Format untuk print soft warning
        elif warning:
            print(Fore.YELLOW + '[{}] '.format(timestamp) + Style.RESET_ALL + '{}'.format(text) + Style.RESET_ALL)
        # Format untuk print message / output
        else:
            print(Fore.GREEN + '[{}] '.format(timestamp) + Style.RESET_ALL + '{}'.format(text) + Style.RESET_ALL)
    timestamp_sd = strftime('%b %d %H:%M:%S', localtime())
    # Format untuk notifikasi status sistem
    if sd_notify:
        sd_notifier.notify('STATUS={} - {}.'.format(timestamp_sd, unidecode(text)))


# Character cleanup (untuk ubah alfabet yang selain latin)
def character_cleanup(alphabet):
    clean = alphabet.strip()
    
    for this, that in [[' ', '-'], ['ä', 'ae'], ['Ä', 'Ae'], ['ö', 'oe'], ['Ö', 'Oe'], ['ü', 'ue'], ['Ü', 'Ue'], ['ß', 'ss']]:
        clean = clean.replace(this, that)
    
    return unidecode(clean)


# MQTT on_connect callback (waktu pertama connecting ke MQTT)
# (src: http://www.eclipse.org/paho/clients/python/docs/#callbacks)
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print_out('Koneksi MQTT berhasil!', console=True, sd_notify=True)
        print()
    else:
        print_out(text='Gagal terkoneksi!, result code: {} - {}'.format(str(rc), mqtt.connack_string(rc)), error=True, sd_notify=True)
        os._exit(1)


# MQTT on_publish callback (waktu publish, bisa dipake untuk kirim ke database nantinya)
def on_publish(client, userdata, mid):
    pass


# Load configuration file
config_dir = parse_args.config_dir

config = ConfigParser(delimiters=('=', ), inline_comment_prefixes=('#'))
config.optionxform = str

try:
    with open(os.path.join(config_dir, 'config.ini')) as config_file:
        config.read_file(config_file)
except IOError:
    print_out('No configuration file "config.ini"', error=True, sd_notify=True)
    sys.exit(1)

default_base_topic = 'miflora'

reporting_mode = config['General'].get('reporting_method', 'mqtt-json')
used_adapter = config['General'].get('adapter', 'hci0')
daemon_enabled = config['Daemon'].getboolean('enabled', True)
base_topic = config['MQTT'].get('base_topic', default_base_topic).lower()
sleep_period = config['Daemon'].getint('period', 300)
miflora_cache_timeout = sleep_period - 1


# Configuration check
if reporting_mode not in ['mqtt-json', 'mqtt-homie', 'json', 'mqtt-smarthome', 'homeassistant-mqtt', 'thingsboard-json', 'wirenboard-mqtt']:
    print_out('Configuration parameter reporting_mode set to an invalid value', error=True, sd_notify=True)
    sys.exit(1)
if not config['Sensors']:
    print_out('No sensors found in configuration file "config.ini"', error=True, sd_notify=True)
    sys.exit(1)
# print_out('Debug: Configuration accepted', console=False, sd_notify=True)


# Inisialisasi MQTT Client
if reporting_mode in ['mqtt-json', 'mqtt-smarthome', 'homeassistant-mqtt', 'thingsboard-json', 'wirenboard-mqtt']:
    print_out('Connecting to MQTT broker ...')
    
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_publish = on_publish

    if reporting_mode == 'mqtt-json':
        mqtt_client.will_set('{}/$announce'.format(base_topic), payload='{}', retain=True)
    elif reporting_mode == 'mqtt-smarthome':
        mqtt_client.will_set('{}/connected'.format(base_topic), payload='0', retain=True)

    if config['MQTT'].getboolean('tls', False):
        # According to the docs, setting PROTOCOL_SSLv23 "Selects the highest protocol version
        # that both the client and server support. Despite the name, this option can select
        # “TLS” protocols as well as “SSL”" - so this seems like a resonable default
        
        # (Src: https://github.com/ThomDietrich/miflora-mqtt-daemon)
        mqtt_client.tls_set(
            ca_certs=config['MQTT'].get('tls_ca_cert', None),
            keyfile=config['MQTT'].get('tls_keyfile', None),
            certfile=config['MQTT'].get('tls_certfile', None),
            tls_version=ssl.PROTOCOL_SSLv23
        )

    mqtt_username = os.environ.get("MQTT_USERNAME", config['MQTT'].get('username'))
    mqtt_password = os.environ.get("MQTT_PASSWORD", config['MQTT'].get('password', None))

    if mqtt_username:
        mqtt_client.username_pw_set(mqtt_username, mqtt_password)
    try:
        mqtt_client.connect(os.environ.get('MQTT_HOSTNAME', config['MQTT'].get('hostname', 'localhost')),
                            port=int(os.environ.get('MQTT_PORT', config['MQTT'].get('port', '1883'))),
                            keepalive=config['MQTT'].getint('keepalive', 60))
    except:
        print_out('Koneksi MQTT error! Silakan cek kembali konfigurasi device "config.ini"', error=True, sd_notify=True)
        sys.exit(1)
    else:
        if reporting_mode == 'mqtt-smarthome':
            mqtt_client.publish('{}/connected'.format(base_topic), payload='1', retain=True)
        if reporting_mode != 'thingsboard-json':
            mqtt_client.loop_start()
            sleep(1.0) # some slack to establish the connection

sd_notifier.notify('READY=1')


# Inisialisasi Sensor
floras = OrderedDict()
for [name, mac] in config['Sensors'].items():
    if not re.match("[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}", mac.lower()):
        print_out('Format MAC Address [{}] salah, silakan cek kembali konfigurasi device "config.ini"!'.format(mac), error=True, sd_notify=True)
        sys.exit(1)

    if '@' in name:
        name_pretty, location_pretty = name.split('@')
    else:
        name_pretty, location_pretty = name, ''

    name_clean = character_cleanup(name_pretty)
    location_clean = character_cleanup(location_pretty)

    flora_dict = OrderedDict()
    print('Menambahkan sensor ke list device dan mencoba koneksi ...')
    print('Name:        "{}"'.format(name_pretty))

    flora_poller = MiFloraPoller(mac=mac, backend=BluepyBackend, cache_timeout=miflora_cache_timeout, adapter=used_adapter)
    
    flora_dict['poller'] = flora_poller
    flora_dict['name_pretty'] = name_pretty
    flora_dict['mac'] = flora_poller._mac
    flora_dict['refresh'] = sleep_period
    flora_dict['location_clean'] = location_clean
    flora_dict['location_pretty'] = location_pretty
    flora_dict['stats'] = {"count": 0, "success": 0, "failure": 0}
    flora_dict['firmware'] = "0.0.0"
    
    try:
        flora_poller.fill_cache()
        flora_poller.parameter_value(MI_LIGHT)
        flora_dict['firmware'] = flora_poller.firmware_version()
    except (IOError, BluetoothBackendException, BTLEException, RuntimeError, BrokenPipeError) as e:
        print_out('Inisialisasi koneksi sensor gagal "{}" [{}] - Due to exception: {}'.format(name_pretty, mac, e), error=True, sd_notify=True)
    else:
        print('Internal Name:   "{}"'.format(name_clean))
        print('Device Name:     "{}"'.format(flora_poller.name()))
        print('MAC Address:     "{}"'.format(flora_poller._mac))
        print('Firmware:        "{}"'.format(flora_poller.firmware_version()))

        if int(flora_poller.firmware_version().replace('.', '')) < 319:
            print_out('Sensor Mi Flora dengan firmware di bawah 3.1.9 sudah tidak disupport, silakan update!', error=True, sd_notify=True)
        print_out('Inisialisasi koneksi sensor berhasil! "{}" [{}]'.format(name_pretty, mac), sd_notify=True)

    print()
    floras[name_clean] = flora_dict


# BLE Discovery (Auto discovery bluetooth dan MQTT broker)
if reporting_mode == 'mqtt-json':
    print_out('Announced Mi Flora devices to MQTT broker for auto-discovery ...')
    flores_info = dict()

    for [flora_name, flora_dict] in floras.items():
        flora_info = {key: value for key, value in flora_dict.items() if key not in ['poller', 'stats']}
        flora_info['topic'] = '{}/{}'.format(base_topic, flora_name)
        flores_info[flora_name] = flora_info
    
    mqtt_client.publish('{}/$announce'.format(base_topic), json.dumps(flores_info), retain=True)
    sleep(0.5)
    print()

# print_out('Debug: Inisialisasi berhasil, MQTT starting publish loop', console=False, sd_notify=True)


# Data Retrieval & Publish (Ambil & Publish data dari sensor)
while True:
    for [flora_name, flora_dict] in floras.items():
        data = OrderedDict()
        attempts = 2
        
        flora_dict['poller']._cache = None
        flora_dict['poller']._last_read = None
        flora_dict['stats']['count'] += 1

        print_out('Mengambil data dari sensor "{}" ...'.format(flora_dict['name_pretty']))

        while attempts != 0 and not flora_dict['poller']._cache:
            try:
                flora_dict['poller'].fill_cache()
                flora_dict['poller'].parameter_value(MI_LIGHT)
            except (IOError, BluetoothBackendException, BTLEException, RuntimeError, BrokenPipeError) as e:
                attempts -= 1
                if attempts > 0:
                    if len(str(e)) > 0:
                        print_out('Gagal... Mencoba kembali karena {}'.format(e), error=True)
                    else:
                        print_out('Mencoba kembali ...', warning=True)
                flora_dict['poller']._cache = None
                flora_dict['poller']._last_read = None

        if not flora_dict['poller']._cache:
            flora_dict['stats']['failure'] += 1
            
            if reporting_mode == 'mqtt-homie':
                mqtt_client[flora_name.lower()].publish('{}/{}/$state'.format(base_topic, flora_name.lower()), 'disconnected', 1, True)

            print_out('Gagal mendapatkan data dari sensor "{}" [{}], success rate: {:.0%}'.format(flora_dict['name_pretty'], flora_dict['mac'], flora_dict['stats']['success']/flora_dict['stats']['count']), error = True, sd_notify = True)
            
            print()
            continue
        else:
            flora_dict['stats']['success'] += 1

        for param,_ in parameters.items():
            data[param] = flora_dict['poller'].parameter_value(param)
        
        # Data result
        data['timestamp'] = {".sv": "timestamp"}
        data['createdAt'] = str(datetime.now(pytz.timezone('Asia/Jakarta')))
        data['bioname'] = 'Solanum lycopersicum var. cerasiforme'
        data['localname'] = 'Cherry Tomato'
        
        sensor_data = json.dumps(data)
        sensor_data_dict = json.loads(sensor_data)

        # output.input['temperature'] = sensor_data_dict['temperature']
        # output.input['light_intensity'] = sensor_data_dict['light']
        # output.input['conductivity'] = sensor_data_dict['conductivity']
        # output.compute()

        # Fuzzy Output
        # fuzzy_out = output.output['condition']

        # Fuzzy Input
        # feature_vector = [sensor_data_dict['temperature'], sensor_data_dict['light'], sensor_data_dict['conductivity'], fuzzy_out]

        # list_data.append((sensor_data_dict, fuzzy_out))

        # Train the random Forest model
        # features = [list(d[0].values())[:3] for d in list_data]  # Taking only sensor values, not battery
        # labels = [d[1] for d in list_data]

        # if len(features) <= 1:
        #     print_out('Insufficient data for model training. At least 2 samples are required.', error=True, sd_notify=True)
        #     continue
        
        # Split
        # try:
        #     X_train, X_test, y_train, y_test = train_test_split(features, labels, test_size=0.2, random_state=42)
        # except ValueError as e:
        #     print_out('ValueError: {}'.format(e), error=True, sd_notify=True)
        #     continue

        # rf_model = RandomForestClassifier(n_estimators=100, random_state=42)

        # try:
        #     rf_model.fit(X_train, y_train)
        # except ValueError as e:
        #     print_out('ValueError: {}'.format(e), error=True, sd_notify=True)
        #     continue
        
        # Evaluate the model
        # accuracy = rf_model.score(X_test, y_test)

        # Push dataset 
        fb_sensor.push().set(sensor_data_dict)
        print_out('Result: {}'.format(sensor_data), sd_notify=True)

        if reporting_mode == 'mqtt-json':
            # print_out('Publishing to MQTT topic "{}/{}"'.format(base_topic, flora_name))
            mqtt_client.publish('{}/{}'.format(base_topic, flora_name), json.dumps(data))
            
            sleep(0.5) # some slack for the publish roundtrip and callback function
        elif reporting_mode == 'json':
            data['timestamp'] = strftime('%Y-%m-%d %H:%M:%S', localtime())
            data['name'] = flora_name
            data['name_pretty'] = flora_dict['name_pretty']
            data['mac'] = flora_dict['mac']
            data['firmware'] = flora_dict['firmware']
            
            print('Data for "{}": {}'.format(flora_name, json.dumps(data)))
        else:
            raise NameError('Unexpected reporting method.')
        print()

    # print_out('Debug: Status messages published', console=False, sd_notify=True)

    if daemon_enabled:
        print_out('Pause ({} seconds) ...'.format(sleep_period))
        sleep(sleep_period)
        print()
    else:
        print_out('Debug: Execution finished in non-daemon-mode', sd_notify=True)
        if reporting_mode == 'mqtt-json':
            mqtt_client.loop_stop()
        sd_notifier.notify('READY=0')
        break
