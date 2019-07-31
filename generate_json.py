# Script to read dat file and generate the appriate JSON
import os
import json

with open('NearbyGalaxies.dat', 'r') as f:
    for i, line in enumerate(f):
        if i < 37:
            continue
        # if i > 37: break  # run only on one file
        print(line)

        name = line[:19].strip()
        if name[0] == '*': name = name[1:]
        # print(name)

        # Right Ascension
        rah = float(line[19:21])
        ram = float(line[22:24])
        ras = float(line[25:29])
        ra = round(15*(rah + ram/60. + ras/3600.),5)
        # print(rah, ram, ras, ra)

        # Declination
        decd = float(line[31:33])
        decm = float(line[34:36])
        decs = float(line[37:39])
        if decd < 0:
            sign = -1
            decd = abs(decd)
        else:
            sign = +1
        dec = round(sign * (decd + decm / 60. + decs / 3600.),5)
        # print(decd, decm, decs, dec)

        # Foreground Reddening
        ebv = float(line[41:45])

        # Distance modulus
        dm = float(line[46:51])
        dm_upp = float(line[53:56])
        dm_low = float(line[58:61])

        # Radial velocity
        rv = float(line[62:68])
        rv_upp = float(line[69:73])
        rv_low = float(line[74:78])

        # V mag
        vm = float(line[79:83])
        vm_upp = float(line[84:87])
        vm_low = float(line[88:91])

        # Position angle
        pa = float(line[92:97])
        pa_upp = float(line[98:102])
        pa_low = float(line[103:107])

        # Projected ellipticity
        el = float(line[108:112])
        el_upp = float(line[113:117])
        el_low = float(line[118:122])

        # Central surface brightness
        muv = float(line[123:127])
        muv_upp = float(line[128:131])
        muv_low = float(line[132:135])

        # Half-light radius
        rh = float(line[136:142])
        rh_upp = float(line[143:148])
        rh_low = float(line[149:154])

        # RV dispersion
        sigma = float(line[155:159])
        sigma_upp = float(line[160:164])
        sigma_low = float(line[165:169])

        json_dict = {'name': name,
                     'ra': [{'value': ra, 'best': 1, 'reference': '', 'unit': 'deg'}],
                     'dec': [{'value': dec, 'best': 1, 'reference': '', 'unit': 'deg'}],
                     'ebv': [{'value': ebv, 'best': 1, 'reference': ''}],
                     'distance_modulus': [{'value': dm,
                                           'error_upper': dm_upp,
                                           'error_lower': dm_low,
                                           'best': 1,
                                           'reference': ''}],
                     'radial_velocity': [{'value': rv,
                                          'error_upper': rv_upp,
                                          'error_lower': rv_low,
                                          'best': 1,
                                          'reference': ''}],
                     'v_mag': [{'value': vm,
                                'error_upper': vm_upp,
                                'error_lower': vm_low,
                                'best': 1, 'unit': 'mag',
                                'reference': ''}],
                     'position_angle': [{'value': pa,
                                         'error_upper': pa_upp,
                                         'error_lower': pa_low,
                                         'best': 1, 'unit': 'deg',
                                         'reference': ''}],
                     'ellipticity': [{'value': el,
                                      'error_upper': el_upp,
                                      'error_lower': el_low,
                                      'best': 1,
                                      'reference': ''}],
                     'surface_brightness': [{'value': muv,
                                             'error_upper': muv_upp,
                                             'error_lower': muv_low,
                                             'best': 1, 'unit': 'mag/sq.arcsec',
                                             'reference': ''}],
                     'half-light_radius': [{'value': rh,
                                            'error_upper': rh_upp,
                                            'error_lower': rh_low,
                                            'best': 1, 'unit': 'arcmin',
                                            'reference': ''}],
                     'stellar_radial_velocity_dispersion': [{'value': sigma,
                                            'error_upper': sigma_upp,
                                            'error_lower': sigma_low,
                                            'best': 1, 'unit': 'km/s',
                                            'reference': ''}],
                     }

        # Clean up missing values
        for k, v in json_dict.copy().items():
            if k == 'name':
                continue

            e = v[0]
            if (e['value'] == 99.99 and e['error_upper'] == 9.99) or \
                (e['value'] == 999.9 and e['error_upper'] == 99.9) or \
                (e['value'] == 99.9 and e['error_upper'] == 99.9) or \
                (e['value'] == 99.9 and e['error_upper'] == 9.9) or \
                (e['value'] == 9.9 and e['error_upper'] == 9.9) or \
                (e['value'] == 99.0 and e['error_upper'] == 9.0) or \
                (e['value'] == 999.0 and e['error_upper'] == 99.0) or \
                (e['value'] == 9.99 and e['error_upper'] == 9.99):
                del json_dict[k]

        json_data = json.dumps(json_dict, indent=4, sort_keys=False)
        # print(json_data)

        # Write to file
        out_dir = 'data'
        filename = name.strip().replace(' ', '_')+'.json'
        with open(os.path.join(out_dir, filename), 'w') as f:
            f.write(json_data)


