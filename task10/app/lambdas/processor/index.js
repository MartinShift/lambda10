const AWSXRay = require('aws-xray-sdk-core');
const AWS = AWSXRay.captureAWS(require('aws-sdk'));
const https = require('https');
const { v4: uuidv4 } = require('uuid');

const dynamodb = new AWS.DynamoDB.DocumentClient();
const TABLE_NAME = process.env.TARGET_TABLE;

AWSXRay.captureHTTPsGlobal(https);
AWSXRay.capturePromise();

const fetchWeatherData = async () => {
  return new Promise((resolve, reject) => {
    const url = 'https://api.open-meteo.com/v1/forecast?latitude=50.4375&longitude=30.5&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m';
    
    https.get(url, (res) => {
      let data = '';
      res.on('data', (chunk) => data += chunk);
      res.on('end', () => resolve(JSON.parse(data)));
    }).on('error', reject);
  });
};

const storeWeatherData = async (data) => {
  const item = {
    id: uuidv4(),
    forecast: {
      elevation: data.elevation,
      generationtime_ms: data.generationtime_ms,
      hourly: {
        temperature_2m: data.hourly.temperature_2m,
        time: data.hourly.time
      },
      hourly_units: {
        temperature_2m: data.hourly_units.temperature_2m,
        time: data.hourly_units.time
      },
      latitude: data.latitude,
      longitude: data.longitude,
      timezone: data.timezone,
      timezone_abbreviation: data.timezone_abbreviation,
      utc_offset_seconds: data.utc_offset_seconds
    }
  };

  await dynamodb.put({
    TableName: TABLE_NAME,
    Item: item
  }).promise();

  return item;
};

exports.handler = async (event) => {
  try {
    const segment = AWSXRay.getSegment();
    
    const weatherData = await AWSXRay.captureAsyncFunc('fetchWeatherData', fetchWeatherData, segment);
    const storedItem = await AWSXRay.captureAsyncFunc('storeWeatherData', async () => storeWeatherData(weatherData), segment);
    
    console.log('Weather data stored successfully');
    return {
      statusCode: 200,
      body: JSON.stringify({ message: 'Weather data stored successfully', item: storedItem })
    };
  } catch (error) {
    console.error('Error:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'An error occurred' })
    };
  }
};