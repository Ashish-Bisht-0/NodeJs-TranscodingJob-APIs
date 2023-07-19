const express = require("express");
const mongoose = require("mongoose");
const bcrypt = require("bcrypt");
const {sign } = require("jsonwebtoken");
const { publishSync, subscribe } = require("pubsub-js");
const axios = require("axios");

// Create the Express application
const app = express();
app.use(express.json());


//mongodb URI
const mongo_uri ="" // provide Mongodb cluster URI





// Create a map of event details
const subscribersMaps = new Map();
/*
key = event_name
value = javascript object

javascript object key = url
value = susbsciber token
*/
// Function to subscribe a URL to an event
function subscribeToEvent(eventName, url) {
  const token = subscribe(eventName, async (msg, eventData) => {
    try {
      // Send a POST request to the subscriber URL with the event data
      await axios.post(url, eventData);
      console.log(
        `\nEvent '${eventName}' sent to URL '${url}' successfully eventData = '${JSON.stringify(
          eventData
        )}'`
      );
    } catch (error) {
      console.error(
        `\nFailed to send event '${eventName}' to URL '${url}':`,
        error
      );
    }
  });

  console.log(`\nSubscribed URL '${url}' to event '${eventName}'`);
  return token;
}
// Function to publish an event
function publishEvent(eventName, eventDetails) {
  publishSync(eventName, eventDetails);
  console.log(`\nEvent '${eventName}' published`);
}



const login_schema = new mongoose.Schema({
  username: { type: String, required: true, unique: true },
  password: { type: String, required: true },
});

// Create a new collection based on the schema
const loginModel = mongoose.model("login_model", login_schema);

const job_schema = new mongoose.Schema({
  jobId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Job",
    default: new mongoose.Types.ObjectId(), // Generate a new ObjectId
    unique: true,
  },
  status: {
    type: String,
    enum: [
      "job_created",
      "job_running",
      "job_aborted",
      "job_inerror",
      "job_completed",
    ],
    default: "job_created",
  },
  createdAt: { type: Date, required: true, default: Date.now },
  updatedAt: { type: Date, required: true, default: Date.now },
  updateMessage: { type: String, default: "null" },
  priority: { type: String, num: ["low", "high", "very high"], default: "low" },
  retries: { type: Number, default: 1, required: true },
  body: { type: mongoose.Schema.Types.Mixed, required: true },
});
// Create a new collection based on the schema
const job_status = mongoose.model("job_schema", job_schema);

async function connect() {
  try {
    await mongoose.connect(mongo_uri);
    console.log("\n\nSuccessfully Connected to DB.....");
    // const password = await bcrypt.hash("abc", 10); // Specify the number of salt rounds
    // const user = new loginModel({
    //   username : "admin",
    //   password: password,
    // });
    // await user.save();

    // const sampleJob = new job_status({body:{
    //   "source": {
    //     "url": "https:/<ip>/<path>/file/input.mp4",
    //     "tracks": [
    //       {
    //         "type": "video",
    //         "name": "video.avc"
    //       },
    //       {
    //         "type": "audio",
    //         "name": "audio.aac"
    //       }
    //     ]
    //   },
    //   "destination": {
    //     "url": "https:/<ip>/<path>/file/output.mov",
    //     "tracks": [
    //       {
    //         "type": "video",
    //         "name": "video.vp9"
    //       },
    //       {
    //         "type": "audio",
    //         "name": "audio.mp3"
    //       }
    //     ]
    //   }
    // }});
    // await sampleJob.save().then((id)=>console.log(id.jobId.toString())).catch(err=>console.log(err));
  } catch (error) {
    console.log(error);
  }
}

// Connect to the MongoDB database
connect();

// Middleware for basic authentication
const basicAuth = async (req, res, next) => {
  const authHeader = req.headers.authorization;

  if (!authHeader || !authHeader.startsWith("Basic ")) {
    res
      .status(401)
      .json({ message: "Authorization header missing or invalid" });
    return;
  }

  const credentials = authHeader.slice(6);
  const decodedCredentials = Buffer.from(credentials, "base64").toString();
  const [username, password] = decodedCredentials.split(":");

  try {
    const user = await loginModel.findOne({ username: username });

    if (!user) {
      return res.status(401).json({ message: "Invalid username or password" });
    }

    const result = await bcrypt.compare(password, user.password);
    if (!result) {
      return res.status(401).json({ message: "Invalid username or password" });
    }

    const token = sign({ username: user.username }, "secret-key", {
      expiresIn: "1h",
    });
    req.token_session = {
      token,
      expiresIn: 3600, // Expiration time in seconds (1 hour in this case)
      tokenType: "Bearer", // Token type (can be any value, commonly 'Bearer')
    };
    next();
  } catch (error) {
    console.log(error);
    return res.status(500).json({ message: "Internal server error" });
  }
};

// Define the route for password validation
app.post("/users/token", basicAuth, (req, res) => {
  console.log("\nSuccess Authorization.....");
  res.json(req.token_session);
});

app.get("/api/jobs/:jobid?", async (req, res) => {
  jobid = req.params.jobid;
  if (jobid) {
    let objectid = null;
    try {
      objectid = new mongoose.Types.ObjectId(jobid);
    } catch (error) {
      // Handle the error (e.g., log the error, set a default value, etc.)
      return res.status(404).json({ message: "JobId Not Found" });
    }
    const job = await job_status.findOne({ jobId: objectid });
    if (!job) {
      return res.status(404).json({ message: "JobId Not Found" });
    } else {
      res.json(job);
    }
  } else {
    const jobs = await job_status.find({});
    return res.json(jobs);
  }
});

//create a new job

app.post("/api/createjob", async (req, res) => {
  // console.log("\ncreatejob.....",req.body);

  let newJob = req.body;

  if (newJob) {
    try {

      const sampleJob = new job_status({ body: newJob });


      let saved_data = await sampleJob.save()

      const uniqueIdString = saved_data.jobId.toString();

      console.log("Job Created Successfully JobId:", uniqueIdString);
  
      return res.status(200).json({"jobID":uniqueIdString})
    } catch (error) {
      console.log(error)
    return res.status(500).json({ message: "Unable to create job" });
    }
  }else{
    return res.status(400).json({"message":"Give Proper Job Body"})
  }
});

app.post("/api/jobs/:jobid/start", async (req, res) => {
  jobid = req.params.jobid;
  const start_state = {
    status: "job_running",
    updatedAt: Date.now(),
    updateMessage: "Reason XYZ",
  };
  try {
    objectid = new mongoose.Types.ObjectId(jobid);
  } catch (error) {
    // Handle the error (e.g., log the error, set a default value, etc.)
    return res.status(404).json({ message: "JobId Not Found" });
  }
  const job = await job_status.findOne({ jobId: objectid });
  if (!job) {
    return res.status(404).json({ message: "JobId Not Found" });
  } else {
    if (job.status.trim() === start_state.status) {
      return res.status(409).json({ message: "Already fulfilled" });
    }
    const updated_job = await job_status.findOneAndUpdate(
      { jobId: objectid },
      { $set: start_state },
      { new: true }
    );
    publishEvent(start_state.status, {
      job_id: updated_job.jobId.toString(),
      status: updated_job.status,
      message: updated_job.updateMessage,
    });
    return res.json(updated_job);
  }
});

app.post("/api/jobs/:jobid/stop", async (req, res) => {
  jobid = req.params.jobid;
  const stop_state = {
    status: "job_aborted",
    updatedAt: Date.now(),
    updateMessage: "Reason XYZ",
  };
  try {
    objectid = new mongoose.Types.ObjectId(jobid);
  } catch (error) {
    // Handle the error (e.g., log the error, set a default value, etc.)
    return res.status(404).json({ message: "JobId Not Found" });
  }
  const job = await job_status.findOne({ jobId: objectid });
  if (!job) {
    return res.status(404).json({ message: "JobId Not Found" });
  } else {
    if (job.status.trim() === stop_state.status) {
      return res.status(409).json({ message: "Already fulfilled" });
    }
    const updated_job = await job_status.findOneAndUpdate(
      { jobId: objectid },
      { $set: stop_state },
      { new: true }
    );
    publishEvent(stop_state.status, {
      job_id: updated_job.jobId.toString(),
      status: updated_job.status,
      message: updated_job.updateMessage,
    });
    return res.json(updated_job);
  }
});

app.delete("/api/deletejob/:jobid?", async (req, res) => {
  console.log("\ndelete job .....");

  jobid = req.params.jobid;

  if (jobid) {
    let objectid = null;

    try {
      objectid = new mongoose.Types.ObjectId(jobid);
    } catch (error) {
      // Handle the error (e.g., log the error, set a default value, etc.)

      return res.status(404).json({ message: "JobId Not Found" });
    }

    const job = await job_status.findOneAndDelete({ jobId: objectid });

    if (!job) {
      return res.status(404).json({ message: "JobId Not Found" });
    } else {
      res.send("job deleted");
    }
  }
});

app.post("/api/actions", (req, res) => {
  const subscriptions = req.body;
  if (
    !subscriptions ||
    !subscriptions["events"] ||
    !Array.isArray(subscriptions["events"]) ||
    !subscriptions["type"] ||
    typeof subscriptions["type"] !== "string" ||
    !subscriptions["url"] ||
    typeof subscriptions["url"] !== "string"
  ) {
    return res.status(400).json({ error: "Invalid request body" });
  }
  const { events, url } = req.body;
  const response_array = [];
  events.forEach((evt) => {
    if (subscribersMaps.has(evt)) {
      obj = subscribersMaps.get(evt);
      if (url in obj) {
        response_array.push({
          [evt]: { message: "Already fulfilled", subscriber_token: obj[url] },
        });
      } else {
        obj[url] = subscribeToEvent(evt, url); //subscriber {url:token}
        subscribersMaps.set(evt, obj);
        response_array.push({
          [evt]: { message: "Subscribed", subscriber_token: obj[url] },
        });
      }
    } else {
      const obj = { [url]: subscribeToEvent(evt, url) }; //subscriber token {url:token}
      subscribersMaps.set(evt, obj);
      response_array.push({
        [evt]: { message: "Subscribed", subscriber_token: obj[url] },
      });
    }
  });
  console.log(subscribersMaps);
  return res.json(response_array);
});

// Start the Express application
app.listen(3000, () => console.log("Server started on port 3000"));
