/**
 * 
 */
package org.fosstrak.hal.impl.caen;

import gnu.io.CommPortIdentifier;

import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.fosstrak.hal.AsynchronousIdentifyListener;
import org.fosstrak.hal.HardwareException;
import org.fosstrak.hal.MemoryBankDescriptor;
import org.fosstrak.hal.MemoryDescriptor;
import org.fosstrak.hal.Observation;
import org.fosstrak.hal.OutOfBoundsException;
import org.fosstrak.hal.ReadPointNotFoundException;
import org.fosstrak.hal.TagDescriptor;
import org.fosstrak.hal.Trigger;
import org.fosstrak.hal.UnsignedByteArray;
import org.fosstrak.hal.UnsupportedOperationException;
import org.fosstrak.hal.transponder.EPCTransponderModel;
import org.fosstrak.hal.transponder.IDType;
import org.fosstrak.hal.transponder.InventoryItem;
import org.fosstrak.hal.transponder.RFTechnology;
import org.fosstrak.hal.transponder.TransponderType;
import org.fosstrak.hal.util.ByteBlock;
import org.fosstrak.hal.util.ResourceLocator;

import com.caen.RFIDLibrary.CAENRFIDException;
import com.caen.RFIDLibrary.CAENRFIDLogicalSource;
import com.caen.RFIDLibrary.CAENRFIDPort;
import com.caen.RFIDLibrary.CAENRFIDReader;
import com.caen.RFIDLibrary.CAENRFIDTag;

/**
 * @author David Figueroa
 * 
 */
public class CaenUSBController implements CaenController {

	static Logger log = Logger.getLogger(CaenUSBController.class);

	/**
	 * Serial port the reader is attached to
	 */
	private String comPort;

	/**
	 * HAL name
	 */
	private String halName;

	/**
	 * Reader from caen api
	 */
	private CAENRFIDReader reader;

	/**
	 * 
	 */
	private boolean isConnected = false;

	/**
	 * The configuration files
	 */
	private String defaultConfigFile = "/props/CaenUSBController_default.xml";
	private String configFile;
	private String epcTransponderModelsConfig;

	/**
	 * The configuration
	 */
	private XMLConfiguration config = null;

	private HashMap<String, InventoryItem> currentInventory = new HashMap<String, InventoryItem>();

	/**
	 * Number of read points
	 */
	private int numberOfSources = 0;

	/**
	 * Logical read points
	 */
	private HashMap<String, String> logicalSources;
	private HashMap<String, String> antennaNames;

	/**
	 * 
	 */
	public CaenUSBController(String halName, String configFile) {
		this.halName = halName;
		this.configFile = configFile;
		try {
			log.debug("trying to initialize " + halName);
			this.initialize();
			log.debug("initialized");
		} catch (Exception e) {
			log.error("Reader initialization failed", e);
		}
	}

	/**
	 * Initialize a reader.
	 * 
	 * @throws HardwareException
	 */
	public void initialize() throws HardwareException {

		// read parameters from configuration file
		this.config = new XMLConfiguration();
		config.setListDelimiter(',');
		URL fileurl = ResourceLocator.getURL(configFile, defaultConfigFile, this.getClass());

		try {

			config.load(fileurl);

			this.comPort = config.getString("comPort");

			this.epcTransponderModelsConfig = config.getString("epcTransponderModelsConfig");

			logicalSources = new HashMap<String, String>();
			antennaNames = new HashMap<String, String>();
			numberOfSources = config.getMaxIndex("logicalSource") + 1;

			if (numberOfSources > 4) {
				numberOfSources = 4;
			}

			for (int i = 0; i < numberOfSources; i++) {
				// key to current read point
				String key = "logicalSource(" + i + ")";

				// read point name
				String logicalSourceName = config.getString(key + ".name");

				log.debug("Property found: " + key + ".name = " + logicalSourceName);

				String[] antennas = config.getStringArray(key + ".antennas");

				for (String antenna : antennas) {
					log.debug("Property found: " + key + ".antennas = " + config.getString(key + ".antennas"));

					logicalSources.put(logicalSourceName, antenna);
					antennaNames.put(antenna, logicalSourceName);
				}
			}

		} catch (ConfigurationException e) {
			String message = "Error in reader property file";
			log.error("initialize: " + message, e);
			throw new HardwareException(message, e);
		}

		// Initialize the reader hardware
		try {

			log.info("  trying port: " + comPort + " ...");
			initReader(comPort);
			log.info("reader initialized.");
			return;

		} catch (HardwareException e) {
			String message = "Error initializing reader";
			log.error("initialize: " + message, e);
			throw new HardwareException(message, e);
		}
	}

	/**
	 * 
	 * @param comPort
	 * @throws HardwareException
	 */
	protected void initReader(String comPort) throws HardwareException {

		try {

			if (reader != null) {
				reader.Disconnect();
			}

		} catch (CAENRFIDException e) {
			String message = "initReader: Error disconnecting from reader";
			log.error(message, e);
		}

		reader = new CAENRFIDReader();

		try {

			reader.Connect(CAENRFIDPort.CAENRFID_RS232, comPort);

			isConnected = true;

			// Updating the value to the connected serial port
			this.comPort = comPort;

		} catch (CAENRFIDException e) {
			String message = "initReader: Port configuration error";
			log.error(message, e);
			throw new HardwareException(message, e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.fosstrak.hal.HardwareAbstraction#addAsynchronousIdentifyListener(
	 * org.fosstrak.hal.AsynchronousIdentifyListener)
	 */
	public void addAsynchronousIdentifyListener(AsynchronousIdentifyListener listener) throws HardwareException,
			UnsupportedOperationException {

		throw new UnsupportedOperationException("addAsynchronousIdentifyListener: Unsupported method");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#getAllParameterNames()
	 */
	public String[] getAllParameterNames() throws HardwareException, UnsupportedOperationException {

		try {
			@SuppressWarnings("rawtypes")
			Iterator it = config.getKeys();
			List<String> names = new Vector<String>();
			Object item;

			while (it.hasNext()) {
				item = it.next();

				if (String.class.isInstance(item)) {
					names.add((String) item);
				}
			}

			String[] namesarray = new String[names.size()];
			namesarray = names.toArray(namesarray);

			return namesarray;

		} catch (Exception e) {
			log.error("getAllParameterNames: Error gettings parameter names", e);
			throw new HardwareException("Error getting parameter names", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#getHALName()
	 */
	public String getHALName() {
		return halName;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#getParameter(java.lang.String)
	 */
	public String getParameter(String param) throws HardwareException, UnsupportedOperationException {

		try {
			// config file parameter
			String value = config.getString(param);
			return value;

		} catch (Exception e) {
			log.error("getParameter: Error getting parameter", e);
			throw new HardwareException("Error getting parameter", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#getReadPointNames()
	 */
	public String[] getReadPointNames() {
		Set<String> keyset = logicalSources.keySet();
		return keyset.toArray(new String[keyset.size()]);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.fosstrak.hal.HardwareAbstraction#getReadPointNoiseLevel(java.lang
	 * .String, boolean)
	 */
	public int getReadPointNoiseLevel(String readPointName, boolean normalize) throws UnsupportedOperationException {

		throw new UnsupportedOperationException("getReadPointNoiseLevel: Unsupported method");
	}

	/**
	 * This method gets the current setting of the RF power expressed in mW. As
	 * power levels cannot be set by readpoint, returns the power level of the
	 * reader. Variables readPointName and normalize are not used.
	 * 
	 * The power level returned by the reader goes from 0 to 199
	 * 
	 * @param readPointName
	 *            could be anything (use empty string)
	 * @param normalize
	 *            could be anything (use false)
	 */
	public int getReadPointPowerLevel(String readPointName, boolean normalize) throws HardwareException {

		try {
			int power = reader.GetPower();
			return power;

		} catch (CAENRFIDException e) {

			log.error("getReadPointPowerLevel: Error getting power level", e);
			throw new HardwareException("getReadPointPowerLevel: Error getting power level", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#identify(java.lang.String[])
	 */
	public Observation[] identify(String[] readPointNames) throws HardwareException {

		currentInventory.clear();

		Observation[] observations = new Observation[readPointNames.length];

		for (int i = 0; i < readPointNames.length; i++) {

			// log.debug("Readpoint: " + readPointNames[i]);

			observations[i] = new Observation();
			observations[i].setHalName(getHALName());
			observations[i].setReadPointName(readPointNames[i]);

			List<InventoryItem> inventory = getInventory(readPointNames[i]);
			List<String> ids = new Vector<String>();
			List<TagDescriptor> tds = new Vector<TagDescriptor>();

			for (InventoryItem item : inventory) {
				String id = item.id;

				if (item.transponderType == TransponderType.EPCclass1Gen2) {

					IDType idType = IDType.getIdType("EPC", config.getString("idTypesConfig"));

					EPCTransponderModel tagModel = item.epcTransponderModel;

					MemoryBankDescriptor[] memoryBankDescriptors = new MemoryBankDescriptor[4];
					memoryBankDescriptors[0] = new MemoryBankDescriptor(tagModel.getReservedSize(),
							tagModel.getReservedReadable(), tagModel.getReservedWriteable());
					memoryBankDescriptors[1] = new MemoryBankDescriptor(tagModel.getEpcSize(),
							tagModel.getEpcReadable(), tagModel.getEpcWriteable());
					memoryBankDescriptors[2] = new MemoryBankDescriptor(tagModel.getTidSize(),
							tagModel.getTidReadable(), tagModel.getTidWriteable());
					memoryBankDescriptors[3] = new MemoryBankDescriptor(tagModel.getUserSize(),
							tagModel.getUserReadable(), tagModel.getUserWriteable());

					MemoryDescriptor memoryDescriptor = new MemoryDescriptor(memoryBankDescriptors);

					TagDescriptor td = new TagDescriptor(idType, memoryDescriptor);

					tds.add(td);
				}

				item.readPoint = readPointNames[i];
				ids.add(id);
				currentInventory.put(id, item);
			}

			int len = ids.size();
			String[] ids_arr = new String[len];
			ids_arr = ids.toArray(ids_arr);

			if (tds.size() == len) {
				TagDescriptor[] tds_arr = new TagDescriptor[len];
				tds_arr = tds.toArray(tds_arr);
				observations[i].setTagDescriptors(tds_arr);
			}
			observations[i].setIds(ids_arr);
			observations[i].setTimestamp(System.currentTimeMillis());
		}

		return observations;
	}

	/**
	 * Throws HardwareException if error occurs.
	 * 
	 * @return List of InventoryItems seen on activated antennas
	 * @throws HardwareException
	 *             if an error occurs
	 */
	synchronized protected List<InventoryItem> getInventory(String sourceName) throws HardwareException {

		List<InventoryItem> inventory = new Vector<InventoryItem>();

		// set transponder type to EPCclass1gen2
		byte trType = (byte) 0x84;

		try {

			if (!isConnected) {
				// Returning empty inventory
				return null;
			}

			CAENRFIDLogicalSource source = reader.GetSource(sourceName);

			CAENRFIDTag[] inventoryTags = source.InventoryTag();

			if (inventoryTags == null) {
				// Returning empty inventory
				//return null;
				return inventory;
			}

			log.debug("Inventory size: " + inventoryTags.length);

			for (CAENRFIDTag caenTag : inventoryTags) {

				InventoryItem item = new InventoryItem();

				item.transponderType = TransponderType.getType(trType);
				log.debug("Item TransType: " + item.transponderType.name());
				item.rfTechnology = RFTechnology.getType(trType);

				// item.tid = caenTag.GetTID(); // Eres el culpable del null
				// Cannot get TID from caen API
				byte[] tid = { (byte) 0x000 };
				item.tid = tid;

				item.epcTransponderModel = EPCTransponderModel.getEpcTrasponderModel(item.tid,
						epcTransponderModelsConfig);

				item.id = ByteBlock.byteArrayToHexString(caenTag.GetId());
				item.readPoint = sourceName;

				inventory.add(item);
			}

			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
			}

			notify();

			return inventory;

		} catch (CAENRFIDException e) {

			isConnected = false;

			throw new HardwareException("getInventory: " + e.getMessage(), e);

		} finally {

			loopUntilReconnect();
		}
	}

	/**
	 * Help to manage the event of a physical reader desconnection tries to
	 * reconnect to all serial ports until the reader is plugged in again
	 */
	private void loopUntilReconnect() {

		@SuppressWarnings("unchecked")
		Enumeration<CommPortIdentifier> portEnum = CommPortIdentifier.getPortIdentifiers();

		while (portEnum.hasMoreElements() && !isConnected) {
			CommPortIdentifier portIdentifier = portEnum.nextElement();

			if (portIdentifier.getPortType() != CommPortIdentifier.PORT_SERIAL) {
				continue;
			}

			log.debug("loopUntilReconnect: trying to connect to port, " + portIdentifier.getName());

			try {
				initReader(portIdentifier.getName());

			} catch (HardwareException e) {
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#isAsynchronousIdentifyRunning()
	 */
	public boolean isAsynchronousIdentifyRunning() throws HardwareException, UnsupportedOperationException {

		throw new UnsupportedOperationException("isAsynchronousIdentifyRunning: Unsupported method");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.fosstrak.hal.HardwareAbstraction#isReadPointReady(java.lang.String)
	 */
	public boolean isReadPointReady(String readPointName) {

		for (String rp : getReadPointNames()) {
			if (rp.equalsIgnoreCase(readPointName))
				return true;
		}

		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#kill(java.lang.String,
	 * java.lang.String, java.lang.String[])
	 */
	public void kill(String readPointName, String id, String[] passwords) throws ReadPointNotFoundException,
			HardwareException, UnsupportedOperationException {

		throw new UnsupportedOperationException("kill: Unsupported method (temporarily)");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#readBytes(java.lang.String,
	 * java.lang.String, int, int, int, java.lang.String[])
	 */
	public UnsignedByteArray readBytes(String readPointName, String id, int memoryBank, int offset, int length,
			String[] passwords) throws ReadPointNotFoundException, OutOfBoundsException, HardwareException,
			UnsupportedOperationException {

		throw new UnsupportedOperationException("readBytes: Unsupported method");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.fosstrak.hal.HardwareAbstraction#removeAsynchronousIdentifyListener
	 * (org.fosstrak.hal.AsynchronousIdentifyListener)
	 */
	public void removeAsynchronousIdentifyListener(AsynchronousIdentifyListener listener) throws HardwareException,
			UnsupportedOperationException {

		throw new UnsupportedOperationException("removeAsynchronousIdentifyListener: Unsupported method");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#reset()
	 */
	public void reset() throws HardwareException {

		log.debug("reset: Caen HAL is going to reset");
		initReader(comPort);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#setParameter(java.lang.String,
	 * java.lang.String)
	 */
	public void setParameter(String param, String value) throws HardwareException {

		try {
			// This method doesn't override reader configuration
			config.setProperty(param, value);
			this.initialize();

		} catch (Exception e) {
			log.error("setParameter: Error setting parameter", e);
			throw new HardwareException("Error setting parameter", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.fosstrak.hal.HardwareAbstraction#shutDownReadPoint(java.lang.String)
	 */
	public void shutDownReadPoint(String readPointName) throws ReadPointNotFoundException, HardwareException,
			UnsupportedOperationException {

		//throw new UnsupportedOperationException("shutDownReadPoint: Unsupported method");
		
		try {
			reader.Disconnect();
			log.info("Shutting down reader: " + readPointName);
			isConnected = false;
			
		} catch (CAENRFIDException e) {
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.fosstrak.hal.HardwareAbstraction#startAsynchronousIdentify(java.lang
	 * .String[], org.fosstrak.hal.Trigger)
	 */
	public void startAsynchronousIdentify(String[] readPointNames, Trigger trigger) throws ReadPointNotFoundException,
			HardwareException, UnsupportedOperationException {

		throw new UnsupportedOperationException("startAsynchronousIdentify: Unsupported method");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.fosstrak.hal.HardwareAbstraction#startUpReadPoint(java.lang.String)
	 */
	public void startUpReadPoint(String readPointName) throws ReadPointNotFoundException, HardwareException,
			UnsupportedOperationException {

		//throw new UnsupportedOperationException("startUpReadPoint: Unsupported method");
		try {
			log.info("Starting up reader: " + reader.GetSource(readPointName).GetName());
			
		} catch (CAENRFIDException e) {
			throw new HardwareException(e.getMessage());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#stopAsynchronousIdentify()
	 */
	public void stopAsynchronousIdentify() throws HardwareException, UnsupportedOperationException {

		throw new UnsupportedOperationException("stopAsynchronousIdentify: Unsupported method");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#supportsAsynchronousIdentify()
	 */
	public boolean supportsAsynchronousIdentify() {
		// TODO Auto-generated method stub
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.fosstrak.hal.HardwareAbstraction#supportsGetReadPointNoiseLevel()
	 */
	public boolean supportsGetReadPointNoiseLevel() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.fosstrak.hal.HardwareAbstraction#supportsGetReadPointPowerLevel()
	 */
	public boolean supportsGetReadPointPowerLevel() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#supportsIsReadPointReady()
	 */
	public boolean supportsIsReadPointReady() {
		// TODO Auto-generated method stub
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#supportsKill()
	 */
	public boolean supportsKill() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#supportsParameters()
	 */
	public boolean supportsParameters() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#supportsReadBytes()
	 */
	public boolean supportsReadBytes() {
		// TODO Auto-generated method stub
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#supportsReset()
	 */
	public boolean supportsReset() {

		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#supportsShutDownReadPoint()
	 */
	public boolean supportsShutDownReadPoint() {
		// TODO Auto-generated method stub
		//return false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#supportsStartUpReadPoint()
	 */
	public boolean supportsStartUpReadPoint() {
		// TODO Auto-generated method stub
		//return false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#supportsWriteBytes()
	 */
	public boolean supportsWriteBytes() {
		// TODO Auto-generated method stub
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#supportsWriteId()
	 */
	public boolean supportsWriteId() {
		// TODO Auto-generated method stub
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#writeBytes(java.lang.String,
	 * java.lang.String, int, int, org.fosstrak.hal.UnsignedByteArray,
	 * java.lang.String[])
	 */
	public void writeBytes(String readPointName, String id, int memoryBank, int offset, UnsignedByteArray data,
			String[] passwords) throws ReadPointNotFoundException, OutOfBoundsException, HardwareException,
			UnsupportedOperationException {

		throw new UnsupportedOperationException("writeBytes: Unsupported method");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.fosstrak.hal.HardwareAbstraction#writeId(java.lang.String,
	 * java.lang.String, java.lang.String[])
	 */
	public void writeId(String readPointName, String id, String[] passwords) throws ReadPointNotFoundException,
			HardwareException, UnsupportedOperationException {

		throw new UnsupportedOperationException("writeId: Unsupported method");
	}

}
