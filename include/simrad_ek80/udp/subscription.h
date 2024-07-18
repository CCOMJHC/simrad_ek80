#ifndef SIMRAD_EK80_SUBSCRIPTION_H
#define SIMRAD_EK80_SUBSCRIPTION_H

#include <mutex>
#include <memory>
#include <vector>
#include <map>
#include <string>

namespace simrad
{

class Subscription
{
public:
  typedef std::shared_ptr<Subscription> Ptr;
  typedef std::weak_ptr<Subscription> WeakPtr;

  Subscription(const std::string& type, const std::string& channel);
    
  virtual ~Subscription();

  void setParameter(std::string key, std::string value);
    
  void setID(int id);
  int getID() const;

  std::string subscribeString();
    
  virtual void addData(std::shared_ptr<std::vector<unsigned char> >& data) = 0;

protected:
  std::string type_;
  std::string channel_;
  std::map<std::string,std::string> parameters_;
  int id_;

private:
  Subscription(const Subscription&);
  Subscription();
};


} // namespace simrad

#endif

