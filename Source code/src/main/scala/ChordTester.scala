
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Terminated}

import scala.collection.mutable
import scala.util.Random
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class ChordTester(numMaxNodes: Int, numRequests: Int) extends Actor with ActorLogging {
  val actorSystem = ActorSystem("PublishSubscribeChord")
  val r = new Random
  val factor: Int = math.ceil(math.log(numMaxNodes / math.log(2))).toInt
  var currentNrMessages: Int = 0
  var currentNrFailedNodes: Int = 0
  val nodesAlive: mutable.HashMap[Int, ActorRef] = new mutable.HashMap[Int, ActorRef]()
  val ids: mutable.HashMap[ActorRef, Int] = new mutable.HashMap[ActorRef, Int]()

  // nó inicial
  var id: Int = r.nextInt(numMaxNodes)
  val actorInit: ActorRef = actorSystem.actorOf(SubscribeChord.props(self), "Initializer")
  context.watch(actorInit)
  actorInit ! create(factor, id, actorInit)
  nodesAlive + (id -> actorInit)
  ids + (actorInit -> id)

  // criação dos nós
  for (_ <- 0 until numMaxNodes) {
    do {
      id = r.nextInt(numMaxNodes)
    } while (nodesAlive contains id)
    if (nodesAlive contains id) println(id + " true") else println(id + " false")
    val chordNode: ActorRef = actorSystem.actorOf(SubscribeChord.props(self), "Node" + id)
    context.watch(chordNode)
    chordNode ! create(factor, id, actorInit)
    nodesAlive + (id -> chordNode)
    ids + (chordNode -> id)
  }

  context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, getRandomNode, NodeFailure) //TODO melhorar, pode resultar em loop infinito

  val messageTypes: List[String] = List("SUBSCRIBE", "PUBLISH", "UNSUBSCRIBE")
  val topics: List[String] = List("Health Care", "Consumer Services", "Energy", "Finance", "Basic Industries",
    "Technology", "Transportation", "Miscellaneous", "Capital Goods", "Public Utilities", "Consumer Durables",
    "Electric Utilities: Central", "Industrial Machinery/Components", "Oil & Gas Production", "Power Generation",
    "Savings Institutions", "Restaurants", "Food Chains", "Natural Gas Distribution", "Packaged Foods",
    "Biotechnology: Biological Products", "Metal Fabrications", "Computer Software: Prepackaged Software",
    "Investment Bankers/Brokers/Service", "RETAIL: Building Materials", "Real Estate Investment Trusts", "Major Banks",
    "Major Pharmaceuticals", "Advertising", "Semiconductors", "Biotechnology: Laboratory Analytical Instruments",
    "Telecommunications Equipment", "Television Services", "Other Specialty Stores", "Specialty Insurers",
    "Consumer Specialties", "Electronic Components", "Home Furnishings", "Package Goods/Cosmetics", "Commercial Banks",
    "Environmental Services", "Auto Parts:O.E.M.", "EDP Services", "Miscellaneous manufacturing industries",
    "Hotels/Resorts", "Ophthalmic Goods", "Business Services", "Precious Metals", "Consumer Electronics/Appliances",
    "Major Chemicals", "Oil Refining/Marketing", "Marine Transportation", "Hospital/Nursing Management")
  val messages: List[String] = List(
    "in purus eu magna vulputate luctus cum sociis natoque penatibus et magnis dis parturient",
    "semper sapien a libero nam dui proin leo odio porttitor id consequat in consequat ut nulla sed accumsan",
    "varius integer ac leo pellentesque ultrices mattis odio donec vitae",
    "et ultrices posuere cubilia curae nulla dapibus dolor vel est donec odio justo sollicitudin ut",
    "amet sapien dignissim vestibulum vestibulum ante ipsum primis in faucibus orci",
    "sit amet sapien dignissim vestibulum vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere",
    "varius nulla facilisi cras non velit nec nisi vulputate nonummy",
    "donec pharetra magna vestibulum aliquet ultrices erat tortor sollicitudin mi sit amet lobortis sapien sapien non mi",
    "consectetuer adipiscing elit proin risus praesent lectus vestibulum quam sapien varius ut blandit non interdum in ante vestibulum ante ipsum",
    "vel enim sit amet nunc viverra dapibus nulla suscipit ligula in lacus curabitur at ipsum ac tellus semper interdum",
    "vel est donec odio justo sollicitudin ut suscipit a feugiat et eros vestibulum ac est lacinia",
    "pede justo eu massa donec dapibus duis at velit eu est",
    "sed tristique in tempus sit amet sem fusce consequat nulla nisl",
    "orci eget orci vehicula condimentum curabitur in libero ut massa volutpat convallis",
    "vestibulum vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae nulla dapibus dolor vel est",
    "est donec odio justo sollicitudin ut suscipit a feugiat et eros vestibulum ac est lacinia nisi",
    "sapien dignissim vestibulum vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae nulla",
    "justo nec condimentum neque sapien placerat ante nulla justo aliquam quis turpis eget elit sodales scelerisque",
    "a ipsum integer a nibh in quis justo maecenas rhoncus aliquam lacus morbi quis tortor id nulla",
    "suspendisse accumsan tortor quis turpis sed ante vivamus tortor duis mattis egestas metus aenean fermentum donec",
    "bibendum imperdiet nullam orci pede venenatis non sodales sed tincidunt eu felis fusce posuere felis sed lacus",
    "massa id nisl venenatis lacinia aenean sit amet justo morbi ut odio cras mi pede malesuada in imperdiet et commodo",
    "mus etiam vel augue vestibulum rutrum rutrum neque aenean auctor gravida sem praesent id massa id nisl venenatis",
    "id pretium iaculis diam erat fermentum justo nec condimentum neque sapien placerat ante nulla justo",
    "ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae mauris viverra diam vitae quam suspendisse potenti nullam porttitor",
    "diam vitae quam suspendisse potenti nullam porttitor lacus at turpis",
    "curae nulla dapibus dolor vel est donec odio justo sollicitudin ut suscipit a feugiat et eros vestibulum",
    "lacinia nisi venenatis tristique fusce congue diam id ornare imperdiet sapien urna pretium nisl",
    "faucibus orci luctus et ultrices posuere cubilia curae mauris viverra diam vitae quam suspendisse potenti nullam porttitor lacus at turpis",
    "accumsan odio curabitur convallis duis consequat dui nec nisi volutpat",
    "iaculis diam erat fermentum justo nec condimentum neque sapien placerat ante nulla justo aliquam quis",
    "congue etiam justo etiam pretium iaculis justo in hac habitasse platea",
    "posuere metus vitae ipsum aliquam non mauris morbi non lectus aliquam sit amet diam in magna",
    "nisi at nibh in hac habitasse platea dictumst aliquam augue quam sollicitudin vitae consectetuer eget rutrum",
    "diam cras pellentesque volutpat dui maecenas tristique est et tempus semper est quam",
    "odio donec vitae nisi nam ultrices libero non mattis pulvinar nulla pede ullamcorper augue a suscipit nulla elit ac nulla",
    "nisi vulputate nonummy maecenas tincidunt lacus at velit vivamus vel nulla eget eros elementum pellentesque quisque porta volutpat erat",
    "elit ac nulla sed vel enim sit amet nunc viverra dapibus nulla suscipit ligula in lacus curabitur at",
    "vitae nisl aenean lectus pellentesque eget nunc donec quis orci eget orci",
    "lectus in quam fringilla rhoncus mauris enim leo rhoncus sed vestibulum sit amet cursus id turpis integer aliquet massa id",
    "ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae donec pharetra magna vestibulum aliquet",
    "amet justo morbi ut odio cras mi pede malesuada in imperdiet et commodo vulputate justo in blandit ultrices enim",
    "fringilla rhoncus mauris enim leo rhoncus sed vestibulum sit amet cursus id",
    "eu mi nulla ac enim in tempor turpis nec euismod scelerisque quam turpis adipiscing lorem vitae mattis nibh",
    "sed accumsan felis ut at dolor quis odio consequat varius integer ac leo pellentesque ultrices mattis",
    "mi sit amet lobortis sapien sapien non mi integer ac neque duis bibendum morbi",
    "auctor sed tristique in tempus sit amet sem fusce consequat nulla nisl nunc nisl duis bibendum felis sed interdum",
    "urna ut tellus nulla ut erat id mauris vulputate elementum nullam varius nulla",
    "quisque erat eros viverra eget congue eget semper rutrum nulla nunc purus phasellus in",
    "morbi vel lectus in quam fringilla rhoncus mauris enim leo rhoncus sed vestibulum sit amet",
    "sed vestibulum sit amet cursus id turpis integer aliquet massa id lobortis convallis",
    "congue diam id ornare imperdiet sapien urna pretium nisl ut volutpat sapien arcu sed augue aliquam",
    "massa volutpat convallis morbi odio odio elementum eu interdum eu tincidunt in leo maecenas pulvinar",
    "est quam pharetra magna ac consequat metus sapien ut nunc vestibulum ante",
    "duis at velit eu est congue elementum in hac habitasse",
    "penatibus et magnis dis parturient montes nascetur ridiculus mus vivamus vestibulum",
    "elementum eu interdum eu tincidunt in leo maecenas pulvinar lobortis est phasellus sit",
    "libero non mattis pulvinar nulla pede ullamcorper augue a suscipit nulla elit ac nulla sed vel enim sit amet nunc",
    "velit vivamus vel nulla eget eros elementum pellentesque quisque porta volutpat erat quisque",
    "sit amet nulla quisque arcu libero rutrum ac lobortis vel dapibus at",
    "consectetuer adipiscing elit proin interdum mauris non ligula pellentesque ultrices phasellus id sapien in sapien",
    "maecenas leo odio condimentum id luctus nec molestie sed justo pellentesque viverra pede ac diam",
    "rhoncus sed vestibulum sit amet cursus id turpis integer aliquet",
    "vestibulum vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae nulla dapibus dolor vel est donec",
    "aliquam lacus morbi quis tortor id nulla ultrices aliquet maecenas leo odio condimentum id luctus nec molestie sed",
    "at nunc commodo placerat praesent blandit nam nulla integer pede justo lacinia eget tincidunt eget tempus vel pede morbi",
    "sapien dignissim vestibulum vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae nulla dapibus dolor",
    "velit donec diam neque vestibulum eget vulputate ut ultrices vel augue vestibulum ante ipsum primis in faucibus",
    "massa tempor convallis nulla neque libero convallis eget eleifend luctus ultricies",
    "lobortis convallis tortor risus dapibus augue vel accumsan tellus nisi eu orci mauris lacinia sapien quis libero",
    "accumsan tortor quis turpis sed ante vivamus tortor duis mattis egestas metus aenean fermentum donec ut",
    "nullam molestie nibh in lectus pellentesque at nulla suspendisse potenti cras in purus eu magna",
    "turpis eget elit sodales scelerisque mauris sit amet eros suspendisse accumsan tortor quis turpis sed ante",
    "mattis egestas metus aenean fermentum donec ut mauris eget massa tempor convallis nulla neque libero convallis eget",
    "aliquam erat volutpat in congue etiam justo etiam pretium iaculis justo",
    "ipsum aliquam non mauris morbi non lectus aliquam sit amet",
    "donec quis orci eget orci vehicula condimentum curabitur in libero",
    "vestibulum sit amet cursus id turpis integer aliquet massa id lobortis convallis tortor risus dapibus augue vel accumsan tellus nisi",
    "in est risus auctor sed tristique in tempus sit amet sem fusce consequat nulla nisl nunc",
    "eget massa tempor convallis nulla neque libero convallis eget eleifend luctus ultricies eu nibh",
    "ut erat id mauris vulputate elementum nullam varius nulla facilisi cras non velit",
    "phasellus id sapien in sapien iaculis congue vivamus metus arcu adipiscing molestie hendrerit",
    "odio in hac habitasse platea dictumst maecenas ut massa quis",
    "cras in purus eu magna vulputate luctus cum sociis natoque penatibus et magnis dis parturient montes nascetur ridiculus mus",
    "duis at velit eu est congue elementum in hac habitasse platea dictumst morbi vestibulum velit id",
    "ligula suspendisse ornare consequat lectus in est risus auctor sed tristique in tempus sit",
    "nulla eget eros elementum pellentesque quisque porta volutpat erat quisque",
    "luctus ultricies eu nibh quisque id justo sit amet sapien dignissim",
    "pede ac diam cras pellentesque volutpat dui maecenas tristique est et tempus semper est quam pharetra",
    "nonummy maecenas tincidunt lacus at velit vivamus vel nulla eget eros elementum pellentesque quisque porta volutpat erat quisque erat",
    "nibh quisque id justo sit amet sapien dignissim vestibulum vestibulum ante ipsum primis in faucibus orci luctus et",
    "lacus at turpis donec posuere metus vitae ipsum aliquam non mauris morbi non lectus aliquam sit amet diam in magna",
    "justo sollicitudin ut suscipit a feugiat et eros vestibulum ac est lacinia nisi venenatis tristique fusce congue diam",
    "eros suspendisse accumsan tortor quis turpis sed ante vivamus tortor duis mattis",
    "ut tellus nulla ut erat id mauris vulputate elementum nullam",
    "elit ac nulla sed vel enim sit amet nunc viverra",
    "eget eleifend luctus ultricies eu nibh quisque id justo sit amet sapien dignissim vestibulum vestibulum ante ipsum primis",
    "neque vestibulum eget vulputate ut ultrices vel augue vestibulum ante ipsum primis in faucibus orci",
    "vulputate ut ultrices vel augue vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae donec pharetra",
    "placerat ante nulla justo aliquam quis turpis eget elit sodales scelerisque mauris sit amet eros suspendisse accumsan tortor quis turpis")

  for (_ <- 0 until numRequests) {
    val randomNode: ActorRef = getRandomNode
    val randomTopic: String = getRandomTopic
    val randomMessageType: String = getRandomMessageType
    var randomMessage: String = ""
    if (randomMessageType.equals("PUBLISH")) {
      randomMessage = getRandomMessage
    }
    randomNode ! sendMessage(randomTopic, randomMessageType, randomMessage)
  }

  def getRandomMessageType: String = {
    val index = r.nextInt(messageTypes.size)
    messageTypes(index)
  }

  def getRandomMessage: String = {
    val index = r.nextInt(messages.size)
    messages(index)
  }

  def getRandomTopic: String = {
    val index = r.nextInt(topics.size)
    topics(index)
  }

  def getRandomNode: ActorRef = {
    var id = -1
    do {
      id = r.nextInt(numMaxNodes)
    } while (!nodesAlive.contains(id))
    nodesAlive(id)
  }

  override def receive: Receive = {
    case Terminated(actor: ActorRef) => {
      val id = ids(actor)
      nodesAlive -= id
      currentNrFailedNodes += 1
      print("node " + id + " failed")
    }
    case CountMessage => currentNrMessages += 1
  }

}