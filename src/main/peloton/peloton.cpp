//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// peloton.cpp
//
// Identification: src/main/peloton/peloton.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include <gflags/gflags.h>

#include "common/init.h"
#include "common/logger.h"
#include "network/network_manager.h"
#include "settings/settings_manager.h"

DECLARE_bool(help);
DEFINE_int32(rep_m,1,"replication_mode for WAL replication");

// Peloton process begins execution here.
int main(int argc, char *argv[]) {
  // Parse the command line flags
  ::google::ParseCommandLineNonHelpFlags(&argc, &argv, true);

  // If "-h" or "-help" is passed in, set up the help messages.
  if (peloton::settings::SettingsManager::GetBool(
          peloton::settings::SettingId::h) ||
      FLAGS_help) {
    FLAGS_help = true;
    ::google::SetUsageMessage("Usage Info: \n");
    ::google::HandleCommandLineHelpFlags();
  }

  // Print settings
  if (peloton::settings::SettingsManager::GetBool(
          peloton::settings::SettingId::display_settings)) {
    auto &settings = peloton::settings::SettingsManager::GetInstance();
    settings.ShowInfo();
  }
  std::cout<<"Replication Mode: "<<FLAGS_rep_m<<std::endl;

  peloton::settings::SettingsManager::SetInt(peloton::settings::SettingId::replication_mode,FLAGS_rep_m);

  try {
    // Setup
    peloton::PelotonInit::Initialize();

    // Create NetworkManager object
    peloton::network::NetworkManager network_manager;

    // Start NetworkManager
    network_manager.StartServer();
  } catch (peloton::ConnectionException &exception) {
    // Nothing to do here!
  }

  // Teardown
  peloton::PelotonInit::Shutdown();

  return 0;
}
