package module;

/**
 * 程序调用入口
 * 
 * @author: hshe-161202
 * @create date: 2017年8月15日
 */
public class OEP {

	public static void main(String[] args) {

		if (args.length != 1) {

			System.out.println("======================== Overview =======================\r\n"
					+ "This is a real-time lineage analyzing program for impala,\r\n"
					+ "corporating with Kafka, Flume and Oracle.\r\n" + "\r\n" + "Version:0.1\r\n"
					+ "========================= Usage =========================\r\n" + "  Input:\r\n"
					+ "    1 for Initializer\r\n" + "    2 for Parser\r\n" + "    3 for DataSlicer\r\n"
					+ "========================== End ==========================");

		} else {

			switch (args[0]) {

			case "1":
				Initializer initializer = new Initializer();
				initializer.doAction();
				break;

			case "2":
				Parser parser = new Parser();
				parser.doAction();
				break;

			case "3":
				DataSlicer dataSlicer = new DataSlicer();
				dataSlicer.doAction();
				break;

			default:
				System.out.println("======================== Overview =======================\r\n"
						+ "This is a real-time lineage analyzing program for impala,\r\n"
						+ "corporating with Kafka, Flume and Oracle.\r\n" + "\r\n" + "Version:0.1\r\n"
						+ "========================= Usage =========================\r\n" + "  Input:\r\n"
						+ "    1 for Initializer\r\n" + "    2 for Parser\r\n" + "    3 for DataSlicer\r\n"
						+ "========================== End ==========================");
			}
		}
	}
}
