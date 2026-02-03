import configparser


def createConfig(location=rf'Config\app_config.ini'):
    config = configparser.ConfigParser()

    config.add_section('Database')
    config.set('Database', 'host', 'localhost')
    config.set('Database', 'port', '5432')

    config.add_section('Logging')
    config.set('Logging', 'level', 'DEBUG')
    config.set('Logging', 'file', 'app.log')

    with open(location, 'w') as configfile:
        config.write(configfile)

    print(f"Configuration file '{location}' created successfully.")


def readConfig(key, section='General', location=rf'Config\app_config.ini'):
    config = configparser.ConfigParser()
    config.read(location)

    return config.get(section, key)


def updateConfig(key, value, section='General', separator=',', overwrite=False, location=rf'Config\app_config.ini'):
    config = configparser.ConfigParser()
    config.read(location)

    if not config.has_section(section):
        config.add_section(section)

    if overwrite or not config.has_option(section, key):
        config.set(section, key, value)
    else:
        existing_value = config.get(section, key)
        new_value = existing_value + separator + value
        config.set(section, key, new_value)

    with open(location, 'w') as configfile:
        config.write(configfile)

    print(f"Configuration '{key}' updated successfully in section '{section}'.")


if __name__ == "__main__":
    createConfig()
    updateConfig('timeout', '30', section='Database')
    timeout = readConfig('timeout', section='Database')
    print(f"Database timeout: {timeout}")

    updateConfig('timeout', '60', section='Database')
    timeout = readConfig('timeout', section='Database')
    print(f"Database timeout after appending: {timeout}")

    updateConfig('timeout', '30', section='Database', overwrite=True)
    timeout = readConfig('timeout', section='Database')
    print(f"Database timeout after overwriting: {timeout}")