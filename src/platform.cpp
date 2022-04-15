#include <simrad_ek80/platform.h>

namespace simrad
{

Platform::Platform(ParameterManager::Ptr& parameter_manager)
  :parameter_manager_(parameter_manager)
{

}

const ParameterGroup::Map& Platform::getParameters()
{
  subscribe();
  return parameters_;
}

void Platform::subscribe()
{
  if(!subscribed_)
  {
    parameters_["position"] = ParameterGroup::Ptr(new ParameterGroup(parameter_manager_));
    parameters_["position"]->add("lat","OwnShip/Latitude",false);
    parameters_["position"]->add("lon","OwnShip/Longitude",false);
        
    parameters_["heading"] = ParameterGroup::Ptr(new ParameterGroup(parameter_manager_));
    parameters_["heading"]->add("heading","OwnShip/Heading",false);
        
    parameters_["attitude"] = ParameterGroup::Ptr(new ParameterGroup(parameter_manager_));
    parameters_["attitude"]->add("roll","OwnShip/Roll");
    parameters_["attitude"]->add("pitch","OwnShip/Pitch");
    parameters_["attitude"]->add("heave","OwnShip/Heave");
    subscribed_ = true;
  }
}

} // namespace simrad
