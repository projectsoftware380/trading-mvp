const { getHistoricalRates } = require('dukascopy-node');

(async () => {
  try {
    const args = process.argv.slice(2);
    const instrument = args[0];
    const from = new Date(args[1]);
    const to = new Date(args[2]);

    const data = await getHistoricalRates({
      instrument: instrument,
      dates: {
        from: from,
        to: to
      },
      timeframe: 'tick',
      format: 'json'
    });
    console.log(JSON.stringify(data));
  } catch (error) {
    console.error('error', error);
    process.exit(1);
  }
})();
