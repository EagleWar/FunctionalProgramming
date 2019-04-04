var nodemailer = require('nodemailer');  //we use nodemailer to send an email

var transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: 'killbeanpro@gmail.com',
    pass: 'fdfwvvpsgsiadjph'
  }
});

var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    client = new kafka.KafkaClient(),
    consumer = new Consumer(
        client,
        [
            { topic: 'alert'} //we look at the topic Alert to get in real time the danger about battery
        ],
        {
            autoCommit: false
        }
    );

    consumer.on('message', function (message) {
        console.log(message.value);

        var mailOptions = {
          from: 'killbeanpro@gmail.com',
          to: 'yanis.oumbiche@efrei.net',
          subject: 'ALERT! DRONE BATTERY!',
          text: message.value //we sent in the mail the value of the tuple which is the coordinates of the drones, battery and temperature
        };

        transporter.sendMail(mailOptions, function(error, info){
          if (error) {
            console.log(error);
          } else {
            console.log('Email sent: ' + info.response);
          }
        });
    });
