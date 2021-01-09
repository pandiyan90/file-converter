package com.ideas;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

@RestController
@RequestMapping
public class FileController {

	private static final Logger LOG = LoggerFactory.getLogger(FileController.class);
	
	private Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
	@PostMapping("/convert")
	public Object convert(@RequestPart("file") MultipartFile file, @RequestParam String outputPath) throws FileNotFoundException, IOException, CsvException {
		LOG.info(file.getResource().getFilename());
		CSVReader csvReader = new CSVReader(new InputStreamReader(file.getInputStream()));
		String[] headers = csvReader.readNext();

		int i=1;
		String[] nextRecord;
		while((nextRecord = csvReader.readNext()) != null){
			Map<String, Object> map = new HashMap<>();
			for(int j=0; j<nextRecord.length; j++) {
				String[] headerParts = headers[j].split("/");

				Object data = nextRecord[j];
				String key = headerParts[0];
				if(headerParts.length>1) {
					Map<String, Object> firstLevelMap = null;
					if(null == map.get(key)) {
						firstLevelMap = new HashMap<>();
					}
					else {
						firstLevelMap = (Map<String, Object>) map.get(key);
					}
					String firstLevelKey = headerParts[1];
					if(headerParts.length>2) {
						Map<String, Object> secondLevelMap = null;
						if(null == firstLevelMap.get(firstLevelKey)) {
							secondLevelMap = new HashMap<>();
						}
						else {
							secondLevelMap = (Map<String, Object>) firstLevelMap.get(firstLevelKey);
						}
						String secondLevelKey = headerParts[2];
						if(headerParts.length>3) {
							Map<String, Object> thirdLevelMap = null;
							if(null == secondLevelMap.get(secondLevelKey)) {
								thirdLevelMap = new HashMap<>();
							}
							else {
								thirdLevelMap = (Map<String, Object>) secondLevelMap.get(secondLevelKey);
							}
							thirdLevelMap.put(headerParts[3], data);
							secondLevelMap.put(secondLevelKey, thirdLevelMap);
						}
						else {
							secondLevelMap.put(secondLevelKey, data);
						}
						firstLevelMap.put(firstLevelKey, secondLevelMap);
					}
					else {
						firstLevelMap.put(firstLevelKey, data);
					}
					map.put(key, firstLevelMap);
				}
				else {
					map.put(key, data);
				}

			}
			File outputFile = new File(outputPath);
			if(!outputFile.exists()) {
				outputFile.mkdirs();
			}
			Path path = Paths.get(outputPath+File.separator+i+".json");
			Files.write(path, gson.toJson(map).getBytes(), StandardOpenOption.CREATE);
			i++;
		}
		csvReader.close();
		return null;
	}
	
}
