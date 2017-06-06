#!/usr/bin/python

''' Data format for X42 Weather Station, based on class example:
[
{
  "guid": "0-ZZZ123456786C",
  "destination": "0-AAA12345678", 
  "eventTime": "2017-05-20T13:39:37.775000Z", 
  "payload": {
     "format": "urn:com:azuresults:x42ws:sensors", 
     "data": {
        "temperature": 103.013042,
        "pressure": 28.761394,
        "humidity": 55.699007,
        "ambient_light": 195.074042,
        "timestamp": "2017-06-05T17:09:52.184000Z"
     }
   }
}
]
'''

import sys
import datetime
import random
import string
import math

# Fixed values
guidStr = "0-ZZZ12345678"
destinationStr = "0-AAA12345678"
formatStr = "urn:com:azuresults:x42ws:sensors"

# Choice for random letter
letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

iotmsg_header = """\
{
  "guid": "%s",
  "destination": "%s", """

iotmsg_eventTime = """\
  "eventTime": "%sZ", """

iotmsg_payload ="""\
  "payload": {
     "reply-to": "",
     "format": "%s", """

iotmsg_data ="""\
     "data": {
      "sensors": [
        {
          "current": %f,
          "senstype": "temperature"
        },
        {
          "current": %f,
          "senstype": "pressure"
        },
        {
          "current": %f,
          "senstype": "humidity"
        },
        {
          "current": %f,
          "senstype": "ambient_light"
        }
      ],
      "timestamp": "%sZ"
     }
   }
}"""

iotmsg_data2 ="""\
     "data": {
        "temperature": %f,
        "pressure": %f,
        "humidity": %f,
        "ambient_light": %f,
        "timestamp": "%sZ"
     }
   }
}"""

iotmsg_data3 ="""\
     "data": {
        "temperature": %f,
        "pressure": %f,
        "humidity": %f,
        "ambient_light": %f,
        "timestamp": "%sZ",
        "latitude": %f,
        "longitude": %f,
        "altitude": %f
     }
   }
}"""

##### Generate JSON output:

def get_model_seasonal(dayOfYear, year):
  # Totally arbitrary model to support IoT Weather Station data generation
  # It will return a seasonal factor to modify a mean value for seasonal variations
  # It takes a day of year (timetuple().tm_yday) and year
  # It returns a number ranging from -1.0 to +1.0 as a sine wave that varies across the year
  # It is designed to return 0 at equinoxes, max (1.0) at the mid-year solstice, and minimum
  #   value (-1.0) at the end-of-year solstice. This corresponds to Northern Hemisphere seasons.
  # To convert for other latitudes would require further complication (not yet supported)
  # For now I will assume industrial lat/lon approximately as in America/New York or Los Angeles
  # The vernal equinox in these latitudes (35-50 deg N) occurs around March 17
  # See https://www.timeanddate.com/astronomy/equinox-not-equal.html for details.
  
  spring = 76 #equinoxes_doy[0] Mar.17 for America/New York for example
  # So:
  # phi(winter) = -1.0 * pi / 2.0
  # phi(0) is here
  # phi(spring=86) =  0.0 * pi / 2.0
  # phi(summer=172) =  1.0 * pi / 2.0
  # phi(fall=258) =    2.0 * pi / 2.0
  # phi(winter'=355) = 3.0 * pi / 2.0
  # Thus phi(t) is linear in t but wraps around at 2*pi
  t = dayOfYear - spring # the zero gos here
  # correct for orbital position due to leap year (solar/daily offset)
  t0 = (year % 4) # rough model really only, ignores the century-divis-by-400 issue
  t0 = t0/4.0
  t += t0
  # convert range-86:280 to 0.0:2*pi
  tmax = 365.24
  phimax = 2.0 * math.pi
  # solve for phi from t/tmax = phi/phimax
  phi = phimax * t / tmax
  # convert to sine wave output
  result = math.sin(phi)
  # normalize to range(0.0, 1.0)
  ##result = (result + 1.0)/2.0
  return result

def get_model_daily(hr, dlvar):
  # convert to dual sine wave output (daytime vs nighttime levels)
  # arbitrary model of daytime lighting that tries to "fake" seasonal variations
  # Input: hour of day (0.0-24.0)
  # Input: variance of sunrise/sunset from model's "standard" times (0600 and 1800)
  #  (dlvar is applied half to each; sense is to shrink (set-rise) time if dlvar<0,
  #   expand it if >0, and no change if ==0)
  hr_sunrise = 6.0 - dlvar/2.0
  hr_sunset = 18.0 + dlvar/2.0
  t = hr - hr_sunrise
  tmax = 12.0 # noon always in this model
  phimax = math.pi # pi radians (180 deg) between "sunrise" and "sunset"
  # solve for phi from t/tmax = phi/phimax
  phi = phimax * t / tmax
  result = math.sin(phi)
  if hr < hr_sunrise or hr > hr_sunset:
    result = result * 0.25 # fractional variation over the night hours
  return result

def get_model_sample(datetime_obj):
  ''' Generate model-based weather sample. '''
  # Plan: get means to vary according to sample date/time AND lat/lon
  # Temp: vary annually, colder in winter months, warmer in summer
  # Press: random is OK for now
  # Humid: random is OK for now (?)
  # Ambient light: vary daily from min to max, with min/max varying annually
  #  to simulate shorter days in winter and longer in summer
  #
  # Random variation here represents distribution about sample mean
  # Plan is to add small variance Gaussian noise to this data
  tt = datetime_obj.timetuple()
  doy = tt.tm_yday # check if Jan 1 is 1 or 0 - algo requires 0
  yr = tt.tm_year
  factor = get_model_seasonal(doy, yr) # range from -1.0 to +1.0
  mean_temp = 70.0 + 32.0 * factor # annual variation from 38.0 to 102.0
  randTemp = random.uniform(-10.0, 10.0) + mean_temp
  randPress = random.uniform(-1.2, 1.2) + 29.0
  randHumid = random.uniform(-40.0, 40.0) + 50.0
  day_len_variance = factor * 2.0 # hours variation in length of model daytime, seasonal
  day_fraction = tt.tm_hour + ((60.0 * tt.tm_min + (tt.tm_sec * 60.0))/3600.0)
  factor2 = get_model_daily(day_fraction, day_len_variance)
  mean_daylight = 100.0 + 90.0 * factor2 # ranges 10:190 max at noon local
  randLight = random.uniform(-10, 10) + mean_daylight # actual range 0-255
  return (randTemp, randPress, randHumid, randLight)

def rebias_sample(data, biasType, bias):
  '''Will add a value to one of the data items in the sample, to create a step-function change. '''
  T, P, H, L, ts = data
  if biasType == 'T':
    T += bias
  if biasType == 'P':
    P += bias
  if biasType == 'H':
    H += bias
  if biasType == 'L':
    L += bias
  return (T, P, H, L, ts)

positions = {}
base_lat = 37.255
base_lon = -122.36
base_alt = 30.0
def get_position(device):
  if device not in positions:
    positions[device] = {
      "latitude": base_lat + random.uniform(-5.0, 5.0),
      "longitude": base_lon + random.uniform(-5.0, 5.0),
      "altitude": base_alt + random.uniform(-25.0, 50.0),
      }
  return positions[device]

def gen_samples(numMsgs, biasType, bias):
  print "["

  dataElementDelimiter = ","
  for counter in range(0, numMsgs):

    randInt = random.randrange(0, 9)
    randLetter = 'B' # random.choice(letters)
    guid = guidStr + str(randInt) + randLetter # choice among 10 random devices
    print iotmsg_header % (guid, destinationStr)

    today = datetime.datetime.today() # local time version
    datestr = today.isoformat()
    print iotmsg_eventTime % (datestr)

    print iotmsg_payload % (formatStr)

    # Generate a set of random floating point numbers for (T,P,H,L)
    # Looking to modify this for Gaussian distributions around step function changes
    # These changes will transition at random times from LOW to HIGH
    # I will attempt to get the data analysis to detect the step changes in real time
    #   and operate the WS relay remotely when they are detected for a given device.
    #
    # For now, just generate the random distribution around a central "fake" mean.
    # This will be replaced by the actual submitted value eventually.
    sampleTime = datetime.datetime.utcnow()
    #(randTemp, randPress, randHumid, randLight) = get_model_sample(today)
    X = get_model_sample(today)
    ts = sampleTime.isoformat()
    X = X + (ts,)
    X = rebias_sample(X, biasType, bias)
    # add simulated GPS position of device
    pos = get_position(guid)
    Y = (pos["latitude"], pos["longitude"], pos["altitude"])
    X = X + Y
    if counter == (numMsgs - 1):
      dataElementDelimiter = ""
    print (iotmsg_data3 % X) + dataElementDelimiter

  print "]"

def test_seasonal_model():
  # Unit testing: print out the seasonal model
  print "Seasonal Model Table"
  for lyn in range(0,5):
    print ""
    print "Table for leapyearnum=", lyn
    for day in range(0,366):
      print "Day = ", day, ", model=", get_model_seasonal(day, lyn)

def test_daily_model():
  # Unit testing: print out daily model
  print "Daily Model Table"
  for var in range(-2,3):
    print ""
    print "Table for variance(hrs)=", var
    for hour in range(0, 4*24):
      dfrac = hour / 4.0
      print "Hour = ", dfrac, ", model=", get_model_daily(dfrac, var)

if __name__=="__main__":
  # Set number of simulated messages to generate
  argc = len(sys.argv)
  numMsgs = int(sys.argv[1]) if argc>1 else 1
  # Set bias for a data type
  biasType = sys.argv[2] if argc>2 else ''
  bias = float(sys.argv[3]) if argc>3 else 0.0
  #generate samples
  gen_samples(numMsgs, biasType, bias)                             
  #test_seasonal_model()
  #test_daily_model()
