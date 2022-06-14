require('dotenv').config()
const got = require('got')
const cron = require('node-cron')
const { providers, Contract, utils } = require('ethers')
const { MongoClient, ServerApiVersion } = require('mongodb')

const client = new MongoClient(process.env.MONGODB_URI, {
	useNewUrlParser: true,
	useUnifiedTopology: true,
	serverApi: ServerApiVersion.v1,
})

function indexCasts() {
	client.connect(async (err) => {
		if (err) {
			console.error('Error connecting to MongoDB.', err)
			return
		}

		const startTime = Date.now()
		const db = client.db('farcaster')
		const connection = db.collection('casts')

		const provider = new providers.AlchemyProvider(
			'rinkeby',
			process.env.ALCHEMY_SECRET
		)

		const registryContract = new Contract(
			'0xe3Be01D99bAa8dB9905b33a3cA391238234B79D1',
			require('./registry-abi.js'),
			provider
		)

		const usersToScrape = ['username']

		for (let i = 0; i < usersToScrape.length; i++) {
			const directoryUrl = await registryContract.getDirectoryUrl(
				utils.formatBytes32String(usersToScrape[i])
			)
			const username = usersToScrape[i]

			try {
				const activityUrl = await got(directoryUrl)
					.json()
					.then((res) => res.body)
					.then((res) => res.addressActivityUrl)

				const activity = await got(activityUrl).json()

				if (activity.length > 0) {
					await connection
						.insertMany(activity)
						.then(() => console.log(`${username} inserted`))
						.catch((err) => {
							console.log(
								`Error saving ${username}'s casts.`,
								err.message
							)
						})
				} else {
					console.log(`${username} has no casts.`)
				}
			} catch (err) {
				console.log(`Error scraping ${username}'s casts.`, err.message)
			}
		}

		client.close()
		const endTime = Date.now()
		const secondsTaken = (endTime - startTime) / 1000
		console.log(
			`Indexed ${usersToScrape.length} users in ${secondsTaken} seconds`
		)
	})
}

indexCasts()
