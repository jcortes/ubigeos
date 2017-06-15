var fs = require('fs');
var util = require('util');

var options = { encoding: 'utf8', flag: 'r' };
var SLASH = '/';
var SPACE = ' ';
var DEP_POS = 0;
var PROV_POS = 1;
var DIST_POS = 2;

fs.createReadStream('ubigeos.txt', options)
	.on('data', onData)
	.on('close', onFileClosed);

function onData(chunk) {
	var lines = chunk
		.replace(/"/g, '')
		.split('\n')
		.filter(function (line) { return line; });

	var departments = lines
		.map(getDepartment)
		.filter(validateCode)
		.reduce(uniqueCodes, []);

	var provinces = lines
		.map(getProvince)
		.filter(validateCode)
		.reduce(uniqueCodes, []);

	var districts = lines
		.map(getDistrict)
		.filter(validateCode)
		.reduce(uniqueCodes, []);

	var ubigeos = departments.map(function (department) {
		var depProvinces = provinces
			.filter(function (province) {
				return province.departmentCode === department.code;
			})
			.map(function (province) {
				var provDistricts = districts
					.filter(function (district) {
						return district.provinceCode === province.code;
					});
				return Object.assign({}, province, { districts: provDistricts });
			});
		return Object.assign({}, department, { provinces: depProvinces});
	});

	console.log('ubigeos:', util.inspect(ubigeos, false, null));
}

function onFileClosed() {
	console.log('File closed');
}

function getDepartment(line) {
	var department = line.split(SLASH)[DEP_POS].trim();
	var code = parseInt(department);
	var name = department && department.substr(department.indexOf(SPACE) + 1);

	return { code: code, name: name };
}

function getProvince(line) {
	var province = line.split(SLASH)[PROV_POS].trim();
	var code = parseInt(province);
	var name = province && province.substr(province.indexOf(SPACE) + 1);

	var department = line.split(SLASH)[DEP_POS].trim();
	var departmentCode = parseInt(department);
	var departmentName = department && department.substr(department.indexOf(SPACE) + 1);

	return {
		code: code, name: name,
		departmentCode: departmentCode,
		departmentName: departmentName
	};
}

function getDistrict(line) {
	var district = line.split(SLASH)[DIST_POS].trim();
	var code = parseInt(district);
	var name = district && district.substr(district.indexOf(SPACE) + 1);

	var province = line.split(SLASH)[PROV_POS].trim();
	var provinceCode = parseInt(province);
	var provinceName = province && province.substr(province.indexOf(SPACE) + 1);

	return {
		code: code, name: name,
		provinceCode: provinceCode,
		provinceName: provinceName
	};
}

function uniqueCodes(prev, next) {
	var found = prev.find(function (item) {
		return item && item.code === next.code;
	});
	return !found ? prev.concat(next) : prev;
}

function validateCode(resource) {
	return !isNaN(parseInt(resource.code));
}